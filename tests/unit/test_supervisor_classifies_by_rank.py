"""``except*`` runs every matching arm, so arm order cannot decide the class.

The supervisor picked a failure class by assigning inside each ``except*`` arm,
with a comment explaining that the AgentIncompatibleError arm came first
deliberately. That reasoning holds for ``except`` and not for ``except*``: PEP
654 runs EVERY arm whose type appears in the group, in source order, so the LAST
match won.

The subclass case was fine, because ``except*`` splits the group and the
incompatible arm consumes it before the ProtocolError arm sees it. The sibling
case was not: two tasks in the connection TaskGroup failing differently.

That is the ordinary shape of the path this ordering exists to protect. The
brain answers with a fatal error frame, the receive task raises
AgentIncompatibleError, and the transport clears its socket reference before
awaiting the close, so a concurrent send raises ConnectionError in the same
group. The cycle was then binned as "connection" and retried on the 1s schedule,
which is the reconnect storm the incompatible class was added to stop.
"""

from __future__ import annotations

import pytest
from z4j_bare.runtime import _prefer_supervisor_failure


def _classify(
    *members: tuple[str, Exception],
) -> tuple[str, BaseException]:
    """Fold matches through the production supervisor classifier."""
    selected: tuple[str, BaseException] | None = None
    for candidate, exception in members:
        selected = _prefer_supervisor_failure(
            selected,
            candidate,
            ExceptionGroup("supervisor match", [exception]),
        )
    assert selected is not None
    return selected


def test_every_matching_arm_runs_which_is_why_order_cannot_decide() -> None:
    """The premise, on the running interpreter rather than from the PEP."""
    ran: list[str] = []
    try:
        raise ExceptionGroup("g", [ValueError("a"), KeyError("b")])
    except* ValueError:
        ran.append("value")
    except* KeyError:
        ran.append("key")
    assert ran == ["value", "key"], "if only one arm ran, this test is obsolete"


def test_an_incompatible_peer_outranks_a_sibling_connection_failure() -> None:
    """The reported failure: a 1s retry where the schedule should be 120s."""
    from z4j_core.errors import AgentIncompatibleError

    incompatible = AgentIncompatibleError("scheduler_upgrade_required")
    classification, representative = _classify(
        ("incompatible", incompatible),
        ("connection", ConnectionError("socket gone")),
    )
    assert classification == "incompatible"
    assert representative is incompatible


def test_order_within_the_group_does_not_change_the_answer() -> None:
    """Which task failed first is arbitrary, so it must not decide the backoff."""
    from z4j_core.errors import AgentIncompatibleError

    incompatible = AgentIncompatibleError("scheduler_upgrade_required")
    classification, representative = _classify(
        ("connection", ConnectionError("socket gone")),
        ("incompatible", incompatible),
    )
    assert classification == "incompatible"
    assert representative is incompatible


@pytest.mark.parametrize(
    ("members", "expected"),
    [
        (["connection"], "connection"),
        (["protocol", "connection"], "protocol"),
        (["auth", "connection"], "auth"),
        (["auth", "incompatible", "protocol", "connection"], "auth"),
    ],
)
def test_the_ranking_holds_across_combinations(members: list[str], expected: str) -> None:
    from z4j_core.errors import AgentIncompatibleError, AuthenticationError, ProtocolError

    build = {
        "auth": lambda: AuthenticationError("bad token"),
        "incompatible": lambda: AgentIncompatibleError("upgrade"),
        "protocol": lambda: ProtocolError("bad frame"),
        "connection": lambda: ConnectionError("socket gone"),
    }
    classification, _representative = _classify(
        *((member, build[member]()) for member in members),
    )
    assert classification == expected


def test_unknown_failure_class_fails_closed() -> None:
    with pytest.raises(KeyError, match="new-class"):
        _prefer_supervisor_failure(
            None,
            "new-class",
            ExceptionGroup("g", [RuntimeError("boom")]),
        )
