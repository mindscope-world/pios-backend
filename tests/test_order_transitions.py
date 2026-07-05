# tests/test_order_transitions.py
"""
Unit tests for Order.transition() — pure model tests, no DB session, no API.

These exercise the state machine directly against a transient (never-flushed)
Order instance, matching how order_service.py calls .transition() before the
row is added to a session.
"""
import pytest

from app.models.all_models import Order, OrderStatus, InvalidTransitionError


def make_order(status: str | None = None) -> Order:
    order = Order(side="BUY", order_type="MARKET", qty=1.0, symbol_id=1)
    order.status = status
    return order


# ── Valid transitions ──────────────────────────────────────────────────────────

def test_new_order_transitions_to_submitted():
    order = make_order(status=None)
    order.transition(OrderStatus.SUBMITTED)
    assert order.status == OrderStatus.SUBMITTED


def test_submitted_transitions_to_filled():
    order = make_order(status=OrderStatus.SUBMITTED)
    order.transition(OrderStatus.FILLED)
    assert order.status == OrderStatus.FILLED


def test_submitted_self_transition_allowed():
    """order_service.py re-enters SUBMITTED when the broker ack isn't an instant fill."""
    order = make_order(status=OrderStatus.SUBMITTED)
    order.transition(OrderStatus.SUBMITTED, "Sent to broker")
    assert order.status == OrderStatus.SUBMITTED


def test_submitted_transitions_to_partial():
    order = make_order(status=OrderStatus.SUBMITTED)
    order.transition(OrderStatus.PARTIAL)
    assert order.status == OrderStatus.PARTIAL


def test_partial_transitions_to_filled():
    order = make_order(status=OrderStatus.PARTIAL)
    order.transition(OrderStatus.FILLED)
    assert order.status == OrderStatus.FILLED


def test_partial_self_transition_allowed():
    """Successive partial fills stay in PARTIAL until fully filled."""
    order = make_order(status=OrderStatus.PARTIAL)
    order.transition(OrderStatus.PARTIAL, "additional partial fill")
    assert order.status == OrderStatus.PARTIAL


def test_new_transitions_to_cancelled():
    order = make_order(status=OrderStatus.NEW)
    order.transition(OrderStatus.CANCELLED, "User requested")
    assert order.status == OrderStatus.CANCELLED


def test_submitted_transitions_to_cancelled():
    """Matches order_service.cancel_order / risk_service.trigger_kill_switch."""
    order = make_order(status=OrderStatus.SUBMITTED)
    order.transition(OrderStatus.CANCELLED, "Kill switch triggered")
    assert order.status == OrderStatus.CANCELLED


def test_partial_transitions_to_cancelled():
    order = make_order(status=OrderStatus.PARTIAL)
    order.transition(OrderStatus.CANCELLED, "User requested")
    assert order.status == OrderStatus.CANCELLED


def test_submitted_transitions_to_rejected():
    order = make_order(status=OrderStatus.SUBMITTED)
    order.transition(OrderStatus.REJECTED, "insufficient margin")
    assert order.status == OrderStatus.REJECTED


def test_new_order_can_go_straight_to_rejected():
    """Risk-gate rejection before ever hitting the broker."""
    order = make_order(status=None)
    order.transition(OrderStatus.REJECTED, "risk gate blocked")
    assert order.status == OrderStatus.REJECTED


# ── Invalid transitions ────────────────────────────────────────────────────────

@pytest.mark.parametrize("terminal_status", [
    OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED,
])
def test_terminal_states_reject_any_further_transition(terminal_status):
    order = make_order(status=terminal_status)
    with pytest.raises(InvalidTransitionError):
        order.transition(OrderStatus.SUBMITTED)
    assert order.status == terminal_status  # unchanged on failure


def test_cancelling_already_filled_order_raises():
    order = make_order(status=OrderStatus.FILLED)
    with pytest.raises(InvalidTransitionError):
        order.transition(OrderStatus.CANCELLED)


def test_new_order_cannot_jump_straight_to_filled():
    order = make_order(status=OrderStatus.NEW)
    with pytest.raises(InvalidTransitionError):
        order.transition(OrderStatus.FILLED)


def test_cancelled_order_cannot_be_resubmitted():
    order = make_order(status=OrderStatus.CANCELLED)
    with pytest.raises(InvalidTransitionError):
        order.transition(OrderStatus.SUBMITTED)


def test_invalid_transition_error_message_contains_both_statuses():
    order = make_order(status=OrderStatus.FILLED)
    with pytest.raises(InvalidTransitionError) as exc_info:
        order.transition(OrderStatus.PARTIAL)
    msg = str(exc_info.value)
    assert "FILLED" in msg
    assert "PARTIAL" in msg


def test_invalid_transition_error_exposes_structured_fields():
    order = make_order(status=OrderStatus.REJECTED)
    with pytest.raises(InvalidTransitionError) as exc_info:
        order.transition(OrderStatus.FILLED)
    err = exc_info.value
    assert err.current_status == OrderStatus.REJECTED
    assert err.new_status == OrderStatus.FILLED


# ── Side effects: state_history + cascaded events ──────────────────────────────

def test_transition_appends_state_history_entry():
    order = make_order(status=OrderStatus.NEW)
    order.transition(OrderStatus.SUBMITTED, "sent to broker")
    assert len(order.state_history) == 1
    entry = order.state_history[0]
    assert entry["from"] == OrderStatus.NEW
    assert entry["to"] == OrderStatus.SUBMITTED
    assert entry["reason"] == "sent to broker"
    assert "at" in entry


def test_transition_appends_order_event():
    order = make_order(status=OrderStatus.NEW)
    order.transition(OrderStatus.SUBMITTED)
    assert len(order.events) == 1
    assert order.events[0].event_type == OrderStatus.SUBMITTED


def test_multiple_transitions_accumulate_history_and_events():
    order = make_order(status=None)
    order.transition(OrderStatus.SUBMITTED)
    order.transition(OrderStatus.PARTIAL)
    order.transition(OrderStatus.FILLED)

    assert [e["to"] for e in order.state_history] == ["SUBMITTED", "PARTIAL", "FILLED"]
    assert [e.event_type for e in order.events] == ["SUBMITTED", "PARTIAL", "FILLED"]


def test_failed_transition_does_not_append_history_or_events():
    order = make_order(status=OrderStatus.FILLED)
    with pytest.raises(InvalidTransitionError):
        order.transition(OrderStatus.CANCELLED)
    assert not order.state_history
    assert len(order.events) == 0
