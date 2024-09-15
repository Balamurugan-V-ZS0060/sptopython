
from config import engine
from sqlalchemy import text
import pandas as pd
import psycopg2

def transfer_balance(sender_id, receiver_id, amount):
    """
    Transfers the specified amount from the sender's account to the receiver's account.

    Args:
        sender_id (int): The ID of the sender's account.
        receiver_id (int): The ID of the receiver's account.
        amount (float): The amount to be transferred.

    Returns:
        None
    """

    # Establish a connection to the database
    conn = engine.connect()

    try:
        # Update sender's balance
        update_sender_sql = text("""
            UPDATE accounts
            SET balance = balance - :amount
            WHERE id = :sender_id
        """)
        conn.execute(update_sender_sql, {"sender_id": sender_id, "amount": amount})
        
        # Update receiver's balance
        update_receiver_sql = text("""
            UPDATE accounts
            SET balance = balance + :amount
            WHERE id = :receiver_id
        """)
        conn.execute(update_receiver_sql, {"receiver_id": receiver_id, "amount": amount})

        # Commit the changes
        conn.commit()

    except psycopg2.Error as e:
        # Rollback the changes in case of an error
        conn.rollback()
        raise Exception(f"Error transferring balance: {e}")

    finally:
        # Close the database connection
        conn.close()

# Example usage
transfer_balance(1, 2, 100.0)
