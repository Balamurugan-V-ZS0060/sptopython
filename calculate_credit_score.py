
from config import engine
from sqlalchemy import text
import pandas as pd
import psycopg2

def update_credit_score(customer_id):
    """
    Updates the customer's credit score in the database based on their loan repayment history and credit card utilization.
    
    Args:
    customer_id (int): The customer's ID.
    
    Returns:
    int: The updated credit score.
    """
    
    # Create a connection to the database
    with engine.connect() as connection:
        
        # Calculate the customer's total loan amount, total repayment, and outstanding balance
        query = text("")
        loan_query = """
            SELECT COALESCE(ROUND(SUM(loan_amount), 2), 0) AS total_loan_amount,
                   COALESCE(ROUND(SUM(repayment_amount), 2), 0) AS total_repayment,
                   COALESCE(ROUND(SUM(outstanding_balance), 2), 0) AS outstanding_loan_balance
            FROM loans
            WHERE loans.customer_id = :customer_id;
        """
        loan_result = connection.execute(loan_query, {"customer_id": customer_id})
        
        # Get the current credit card balance
        credit_card_query = """
            SELECT COALESCE(ROUND(SUM(balance), 2), 0) AS credit_card_balance
            FROM credit_cards
            WHERE credit_cards.customer_id = :customer_id
        """
        credit_card_result = connection.execute(credit_card_query, {"customer_id": customer_id})
        
        # Count the number of late payments
        late_payment_query = """
            SELECT COUNT(*) AS late_payment_count
            FROM payments
            WHERE payments.customer_id = :customer_id AND status = 'Late'
        """
        late_payment_result = connection.execute(late_payment_query, {"customer_id": customer_id})
        
        # Calculate the credit score
        total_loan_amount = loan_result.fetchone()["total_loan_amount"]
        total_repayment = loan_result.fetchone()["total_repayment"]
        outstanding_loan_balance = loan_result.fetchone()["outstanding_loan_balance"]
        credit_card_balance = credit_card_result.fetchone()["credit_card_balance"]
        late_pay_count = late_payment_result.fetchone()["late_payment_count"]
        
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
        
        # Ensure the score stays within reasonable bounds
        if credit_score < 300:
            credit_score = 300
        elif credit_score > 850:
            credit_score = 850
        
        # Update the customer's credit score in the database
        update_query = """
            UPDATE customers
            SET credit_score = :credit_score
            WHERE customers.id = :customer_id;
        """
        connection.execute(update_query, {"credit_score": round(credit_score), "customer_id": customer_id})
        
        # Optionally, log the result or raise an alert for very low scores
        if credit_score < 500:
            alert_query = """
                INSERT INTO credit_score_alerts (customer_id, credit_score, created_at)
                VALUES (:customer_id, :credit_score, NOW());
            """
            connection.execute(alert_query, {"customer_id": customer_id, "credit_score": round(credit_score)})
        
        # Commit the changes
        connection.commit()
        
        return round(credit_score)

# Usage
customer_id = 12345
updated_score = update_credit_score(customer_id)
print("Updated credit score:", updated_score)
