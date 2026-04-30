"""Framework-agnostic declarative scheduler reconciler (1.2.2+).

Shared by every framework adapter (z4j-django, z4j-flask, z4j-fastapi).
Each adapter's own ``declarative.py`` is a thin shim that:

1. Reads ``Z4J_SCHEDULES`` (and optional ``CELERY_BEAT_SCHEDULE``)
   from the framework's config surface (Django settings, Flask
   ``app.config``, FastAPI lifespan kwarg).
2. Calls :func:`reconcile` here with the dicts.
3. Returns a :class:`ReconcileResult` for logging / CLI output.

This module owns:

- the z4j-native dict → :class:`ScheduleSpec` translator
- the :class:`ScheduleSpec` + per-spec source-hash → brain-shaped
  ``ImportedScheduleIn`` payload builder
- the :class:`ScheduleReconciler` HTTP client (httpx-based) that
  POSTs to ``:diff`` (dry-run) or ``:import`` (apply)

Lives in z4j-bare (not z4j-core) because z4j-core is the pure
Pydantic + stdlib core; httpx + the brain-side endpoint shape are
runtime concerns that belong with the agent code.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import re
from typing import Any
from urllib.parse import quote

import httpx

from z4j_core.celerybeat_compat import (
    ScheduleSpec,
    parse_celery_beat_entries,
)

logger = logging.getLogger("z4j.agent.declarative")


#: Project-slug regex matching the brain's own
#: ``api/projects.py`` validation (``^[a-z0-9][a-z0-9-]{1,48}[a-z0-9]$``).
#: Used by :class:`ScheduleReconciler` to validate the operator-
#: supplied slug before it lands in the URL path.
_SLUG_PATTERN: re.Pattern[str] = re.compile(
    r"^[a-z0-9][a-z0-9-]{1,48}[a-z0-9]$",
)


def _redact_response_body(body: str, limit: int = 200) -> str:
    """Trim + redact a response body for safe logging.

    Audit fix HIGH-10: reconciler error logs used to dump the
    full first 500 bytes of the response. If the brain echoed
    the request payload back in a 422 (some FastAPI validation
    paths do), an operator-supplied secret embedded in
    ``args``/``kwargs`` (e.g. webhook URLs with bearer tokens
    in the path) would land verbatim in the log file. We now
    cap to a much smaller window AND collapse anything that
    looks like a Bearer/secret/key value.
    """
    snippet = (body or "")[:limit]
    # Collapse common secret-bearing patterns. These are best-
    # effort — the right answer is "don't log payloads at all"
    # but operators rely on the brain's error message text for
    # debugging, so we keep the snippet and redact the obvious.
    snippet = re.sub(
        r"(?i)(authorization|bearer|password|secret|token|api[_-]?key)"
        r"[\"':=\s]+[^\s,}\"']{4,}",
        r"\1=<redacted>",
        snippet,
    )
    if len(body or "") > limit:
        snippet += "...<truncated>"
    return snippet


@dataclasses.dataclass(slots=True)
class ReconcileResult:
    """Summary of one reconcile call.

    ``inserted`` / ``updated`` / ``unchanged`` / ``failed`` /
    ``deleted`` mirror the brain's :class:`ImportSchedulesResponse`.
    ``dry_run=True`` means the brain returned a diff but did not
    persist changes.
    """

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0
    deleted: int = 0
    errors: dict[int, str] = dataclasses.field(default_factory=dict)
    dry_run: bool = False


def _z4j_native_schedules_to_specs(
    schedules: dict[str, dict[str, Any]],
) -> list[ScheduleSpec]:
    """Translate z4j-native ``Z4J_SCHEDULES`` shape to ScheduleSpec."""
    specs: list[ScheduleSpec] = []
    for name, entry in schedules.items():
        if not isinstance(entry, dict):
            logger.warning(
                "z4j declarative: Z4J_SCHEDULES entry %r is not a dict, "
                "skipping",
                name,
            )
            continue
        task = entry.get("task") or entry.get("task_name")
        kind = entry.get("kind")
        expression = entry.get("expression") or entry.get("schedule")
        if not task:
            logger.warning(
                "z4j declarative: entry %r missing 'task', skipping", name,
            )
            continue
        if not kind or not expression:
            logger.warning(
                "z4j declarative: entry %r missing 'kind' or 'expression', "
                "skipping",
                name,
            )
            continue
        specs.append(
            ScheduleSpec(
                name=name,
                task_name=str(task),
                kind=str(kind),
                expression=str(expression),
                args=list(entry.get("args") or []),
                kwargs=dict(entry.get("kwargs") or {}),
                queue=entry.get("queue"),
                timezone=str(entry.get("timezone") or "UTC"),
            ),
        )
    return specs


def _spec_to_brain_payload(
    spec: ScheduleSpec,
    *,
    engine: str,
    scheduler: str | None,
    source: str,
) -> dict[str, Any]:
    """Build the ``ImportedScheduleIn`` row the brain expects.

    ``source_hash`` is a SHA-256 of canonical JSON of the schedule's
    content; the brain uses it for idempotency: re-running the
    reconciler with no changes is a no-op (same hash → unchanged).

    Canonicalization contract (audit fix LOW):

    - Keys are sorted recursively at every level (``sort_keys=True``
      applies to nested dicts as well per stdlib).
    - List order is significant — ``args=[1, 2]`` and ``args=[2, 1]``
      hash differently because Python's ``json.dumps`` preserves
      list order.
    - Numeric type is significant — ``1`` and ``1.0`` hash
      differently. The translator preserves operator-supplied types
      verbatim.
    - ``None`` vs missing key: every documented field is always
      written into the canonical dict (``queue=None`` etc.), so a
      missing field cannot collide with an explicit ``None``.
    - Encoding: UTF-8 with default ASCII escaping (the brain's
      input validator rejects control chars in ``expression`` /
      ``task_name`` so the canonical-form character set is
      already restricted upstream).

    A future change to ANY of those rules is a breaking change to
    the source_hash and must be co-ordinated with a brain-side
    migration that re-canonicalizes existing ``source_hash``
    values.
    """
    canonical = json.dumps(
        {
            "name": spec.name,
            "task_name": spec.task_name,
            "kind": spec.kind,
            "expression": spec.expression,
            "args": spec.args,
            "kwargs": spec.kwargs,
            "queue": spec.queue,
            "timezone": spec.timezone,
            "engine": engine,
            "scheduler": scheduler,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    payload: dict[str, Any] = {
        "name": spec.name,
        "engine": engine,
        "kind": spec.kind,
        "expression": spec.expression,
        "task_name": spec.task_name,
        "timezone": spec.timezone,
        "queue": spec.queue,
        "args": spec.args,
        "kwargs": spec.kwargs,
        "is_enabled": True,
        "source": source,
        "source_hash": source_hash,
    }
    if scheduler is not None:
        payload["scheduler"] = scheduler
    return payload


class ScheduleReconciler:
    """One-shot reconciler that POSTs translated specs to the brain.

    Framework-agnostic: every adapter (django, flask, fastapi)
    constructs one of these with the brain URL + project-scoped
    API key and calls :meth:`reconcile`.
    """

    def __init__(
        self,
        *,
        brain_url: str,
        api_key: str,
        project_slug: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.brain_url = brain_url.rstrip("/")
        self.api_key = api_key
        # Audit fix HIGH-9: project_slug is operator-supplied
        # config (not network input), but a typo or legacy slug
        # containing ``/``, ``?``, or ``#`` would otherwise break
        # the URL path (silently routing to a different brain
        # endpoint). Validate up-front against the brain's slug
        # regex so the error is loud and local rather than a
        # confusing 404 from the brain.
        if not _SLUG_PATTERN.match(project_slug):
            raise ValueError(
                f"project_slug {project_slug!r} is not a valid slug "
                f"(must match {_SLUG_PATTERN.pattern}). The brain "
                f"will reject any non-conforming slug; this check "
                f"raises locally so the misconfiguration is loud.",
            )
        self.project_slug = project_slug
        # Pre-compute the URL-quoted form. Belt-and-suspenders: the
        # validate-on-construct above already ensures it's safe, but
        # quoting defends against future regex relaxations.
        self._slug_quoted = quote(project_slug, safe="")
        self.timeout = timeout_seconds

    def collect_specs(
        self,
        *,
        z4j_schedules: dict[str, dict[str, Any]] | None,
        celery_beat_schedules: dict[str, dict[str, Any]] | None,
    ) -> list[ScheduleSpec]:
        """Return the union of native + celery-beat specs.

        Native entries win on name conflict; the operator writing
        ``Z4J_SCHEDULES["foo"]`` is being explicit about z4j syntax,
        and the celery-beat translation is a fallback. We log a
        warning when a conflict drops a celery-beat entry.
        """
        specs: dict[str, ScheduleSpec] = {}
        if z4j_schedules:
            for s in _z4j_native_schedules_to_specs(z4j_schedules):
                specs[s.name] = s
        if celery_beat_schedules:
            for s in parse_celery_beat_entries(celery_beat_schedules):
                if s.name in specs:
                    logger.warning(
                        "z4j declarative: schedule %r exists in both "
                        "Z4J_SCHEDULES and CELERY_BEAT_SCHEDULE; the "
                        "Z4J_SCHEDULES entry wins.",
                        s.name,
                    )
                    continue
                specs[s.name] = s
        return list(specs.values())

    def _build_request_body(
        self,
        specs: list[ScheduleSpec],
        *,
        engine: str,
        scheduler: str | None,
        source: str,
    ) -> dict[str, Any]:
        return {
            "schedules": [
                _spec_to_brain_payload(
                    s, engine=engine, scheduler=scheduler, source=source,
                )
                for s in specs
            ],
            "mode": "replace_for_source",
            "source_filter": source,
        }

    def reconcile(
        self,
        *,
        z4j_schedules: dict[str, dict[str, Any]] | None = None,
        celery_beat_schedules: dict[str, dict[str, Any]] | None = None,
        engine: str = "celery",
        scheduler: str | None = None,
        source: str = "declarative",
        dry_run: bool = False,
    ) -> ReconcileResult:
        """Translate config + reconcile against the brain.

        ``scheduler=None`` lets the brain fall back to the project's
        ``default_scheduler_owner`` (1.2.2+) per-row.

        ``dry_run=True`` calls the ``:diff`` endpoint instead of
        ``:import`` so no audit rows are written.
        """
        specs = self.collect_specs(
            z4j_schedules=z4j_schedules,
            celery_beat_schedules=celery_beat_schedules,
        )
        body = self._build_request_body(
            specs, engine=engine, scheduler=scheduler, source=source,
        )

        if dry_run:
            return self._call_diff(body)
        return self._call_import(body)

    def _http_client(self) -> httpx.Client:
        # Audit fix LOW: 1 retry on transient transport errors so
        # a brief brain restart during deploy doesn't fail an
        # otherwise-idempotent ``:diff`` / ``:import``. ``retries=1``
        # only retries on connection-level failures (DNS, TCP
        # reset, TLS) — NOT on 5xx responses (which httpx returns
        # to the caller for explicit handling).
        transport = httpx.HTTPTransport(retries=1)
        return httpx.Client(
            base_url=self.brain_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
            transport=transport,
        )

    def _call_import(self, body: dict[str, Any]) -> ReconcileResult:
        with self._http_client() as client:
            r = client.post(
                f"/api/v1/projects/{self._slug_quoted}/schedules:import",
                json=body,
            )
        if r.status_code >= 400:
            logger.error(
                "z4j declarative: brain rejected import: status=%s body=%s",
                r.status_code, _redact_response_body(r.text),
            )
            return ReconcileResult(failed=len(body["schedules"]))
        data = r.json()
        return ReconcileResult(
            inserted=data.get("inserted", 0),
            updated=data.get("updated", 0),
            unchanged=data.get("unchanged", 0),
            failed=data.get("failed", 0),
            deleted=data.get("deleted", 0),
            errors={int(k): v for k, v in (data.get("errors") or {}).items()},
        )

    def _call_diff(self, body: dict[str, Any]) -> ReconcileResult:
        """POST to ``:diff`` and return a dry-run summary.

        The brain's ``DiffSchedulesResponse`` does NOT carry a
        ``failed`` count: ``:diff`` is a preview, not an apply,
        and per-row validation fires at ``:import`` time. A
        non-2xx response from ``:diff`` (request was malformed)
        is reported by setting ``failed = len(schedules)`` so
        operators see "all of them would have failed" rather
        than misleading zeros.
        """
        with self._http_client() as client:
            r = client.post(
                f"/api/v1/projects/{self._slug_quoted}/schedules:diff",
                json=body,
            )
        if r.status_code >= 400:
            logger.error(
                "z4j declarative dry-run: brain rejected diff: "
                "status=%s body=%s",
                r.status_code, _redact_response_body(r.text),
            )
            return ReconcileResult(failed=len(body["schedules"]), dry_run=True)
        data = r.json()
        return ReconcileResult(
            inserted=data.get("insert", 0),
            updated=data.get("update", 0),
            unchanged=data.get("unchanged", 0),
            deleted=data.get("delete", 0),
            dry_run=True,
        )


__all__ = [
    "ReconcileResult",
    "ScheduleReconciler",
    "_spec_to_brain_payload",
    "_z4j_native_schedules_to_specs",
]
