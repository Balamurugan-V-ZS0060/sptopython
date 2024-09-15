
from config import engine
from sqlalchemy import text
import pandas as pd
import psycopg2

def transfer_balance(p_sender, p_receiver, p_amount):
    """
    Transfers the specified amount from the sender's account to the receiver's account.

    Args:
        p_sender (int): The ID of the sender's account.
        p_receiver (int): The ID of the receiver's account.
        p_amount (float): The amount to transfer.

    Returns:
        None
    """

    # Create a connection object from the engine
    with engine.connect() as connection:
        # Define the SQL queries
        sql_subtract_from_sender = text("""
            UPDATE accounts
            SET balance = balance - :p_amount
            WHERE id = :p_sender;
        """)

        sql_add_to_receiver = text("""
            UPDATE accounts
            SET balance = balance + :p_amount
            WHERE id = :p_receiver;
        """)

        # Execute the queries, passing arguments as a dictionary
        connection.execute(sql_subtract_from_sender, p_sender=p_sender, p_amount=p_amount)
        connection.execute(sql_add_to_receiver, p_receiver=p_receiver, p_amount=p_amount)

        # Commit the changes
        connection.commit()

# Example usage
transfer_balance(1, 2, 100.0)
