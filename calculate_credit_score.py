
from config import engine
from sqlalchemy import text
import pandas as pd
import psycopg2

def calculate_credit_score(p_customer_id):
    """
    Calculate the credit score for a given customer.

    Args:
    p_customer_id (int): The customer ID.

    Returns:
    int: The calculated credit score.
    """
    try:
        conn = engine.connect()
        transaction = conn.begin()

        # Step 1: Calculate the customer's total loan amount, total repayment, and outstanding balance
        query = text("""
            SELECT 
                COALESCE(ROUND(SUM(loan_amount), 2), 0) AS total_loan_amount,
                COALESCE(ROUND(SUM(repayment_amount), 2), 0) AS total_repayment,
                COALESCE(ROUND(SUM(outstanding_balance), 2), 0) AS outstanding_loan_balance
            FROM loans
            WHERE customer_id = :customer_id
        """)
        result = conn.execute(query, {"customer_id": p_customer_id}).first()
        total_loan_amount = result.total_loan_amount
        total_repayment = result.total_repayment
        outstanding_loan_balance = result.outstanding_loan_balance

        # Step 2: Get the current credit card balance
        query = text("""
            SELECT COALESCE(ROUND(SUM(balance), 2), 0) AS credit_card_balance
            FROM credit_cards
            WHERE customer_id = :customer_id
        """)
        result = conn.execute(query, {"customer_id": p_customer_id}).first()
        credit_card_balance = result.credit_card_balance

        # Step 3: Count the number of late payments
        query = text("""
            SELECT COUNT(*) AS late_pay_count
            FROM payments
            WHERE customer_id = :customer_id AND status = 'Late'
        """)
        result = conn.execute(query, {"customer_id": p_customer_id}).first()
        late_pay_count = result.late_pay_count

        # Step 4: Basic rule-based calculation of the credit score
        credit_score = 0
        if total_loan_amount > 0:
            credit_score += round((total_repayment / total_loan_amount) * 400, 2)
        else:
            credit_score += 400

        if credit_card_balance > 0:
            credit_score += round((1 - (credit_card_balance / 10000)) * 300, 2)
        else:
            credit_score += 300

        credit_score -= late_pay_count * 50

        if credit_score < 300:
            credit_score = 300
        elif credit_score > 850:
            credit_score = 850

        # Step 5: Update the customer’s credit score in the database
        query = text("""
            UPDATE customers
            SET credit_score = :credit_score
            WHERE id = :customer_id
        """)
        conn.execute(query, {"credit_score": round(credit_score, 0), "customer_id": p_customer_id})

        # Optionally, log the result or raise an alert for very low scores
        if credit_score < 500:
            query = text("""
                INSERT INTO credit_score_alerts (customer_id, credit_score, created_at)
                VALUES (:customer_id, :credit_score, NOW())
            """)
            conn.execute(query, {"customer_id": p_customer_id, "credit_score": round(credit_score, 0)})

        transaction.commit()
        return round(credit_score, 0)

    except psycopg2.Error as e:
        transaction.rollback()
        raise e
    finally:
        conn.close()

# Usage example
p_customer_id = 123
credit_score = calculate_credit_score(p_customer_id)
print(credit_score)
