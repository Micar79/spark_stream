"""Stateful fraud detection using group state."""
from pyspark.sql.streaming import GroupState, GroupStateTimeout

def fraud_detection(user_id, rows, state: GroupState):
    """Detect fraudulent transaction patterns using stateful aggregation.
    
    Identifies potential fraud by comparing current transaction count
    against historical patterns (3x increase triggers alert).
    
    Args:
        user_id: The user identifier
        rows: Iterator of current transaction rows for the user
        state: GroupState object for storing user history
        
    Yields:
        Tuple of (user_id, total_amount, transaction_count, risk_flag)
    """
    # Retrieve historical transaction count from state
    previous_count = state.get("txn_count") if state.exists else 0

    current_total = 0
    current_count = 0

    # Aggregate current transactions
    for row in rows:
        current_total += row.total_amount
        current_count += row.txn_count

    # Flag as risk if transaction count increases by 3x
    risk_flag = previous_count > 0 and current_count > previous_count * 3

    # Update state for next window
    state.update({"txn_count": current_count})
    state.setTimeoutDuration("30 minutes")

    yield (user_id, current_total, current_count, risk_flag)
