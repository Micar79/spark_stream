from pyspark.sql.streaming import GroupState, GroupStateTimeout

def fraud_detection(user_id, rows, state: GroupState):

    previous_count = state.get("txn_count") if state.exists else 0

    current_total = 0
    current_count = 0

    for row in rows:
        current_total += row.total_amount
        current_count += row.txn_count

    risk_flag = previous_count > 0 and current_count > previous_count * 3

    state.update({"txn_count": current_count})
    state.setTimeoutDuration("30 minutes")

    yield (user_id, current_total, current_count, risk_flag)
