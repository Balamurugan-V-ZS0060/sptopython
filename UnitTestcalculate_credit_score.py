
import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from psycopg2.extensions import cursor
from sqlalchemy import text
from your_module import calculate_credit_score
import pandas as pd

class TestCreditScoreCalculator(unittest.TestCase):

    def setUp(self):
        self.conn = MagicMock(spec=Connection)
        self.cursor = MagicMock(spec=cursor)
        self.conn.execute.return_value = self.cursor
        self.cursor.first.return_value = None
        self.engine = MagicMock(spec=Engine)
        self.engine.connect.return_value = self.conn

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_engine_connect(self, mock_engine):
        calculate_credit_score(123)
        mock_engine.connect.assert_called_once()

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_begin_on_connection(self, mock_engine):
        calculate_credit_score(123)
        self.conn.begin.assert_called_once()

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_execute_on_connection_with_loan_query(self, mock_engine):
        calculate_credit_score(123)
        self.conn.execute.assert_any_call(text(
            "SELECT COALESCE(ROUND(SUM(loan_amount), 2), 0) AS total_loan_amount, "
            "COALESCE(ROUND(SUM(repayment_amount), 2), 0) AS total_repayment, "
            "COALESCE(ROUND(SUM(outstanding_balance), 2), 0) AS outstanding_loan_balance "
            "FROM loans WHERE customer_id = :customer_id"), 
            {"customer_id": 123})

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_execute_on_connection_with_credit_card_query(self, mock_engine):
        calculate_credit_score(123)
        self.conn.execute.assert_any_call(text(
            "SELECT COALESCE(ROUND(SUM(balance), 2), 0) AS credit_card_balance "
            "FROM credit_cards WHERE customer_id = :customer_id"), 
            {"customer_id": 123})

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_execute_on_connection_with_late_pay_query(self, mock_engine):
        calculate_credit_score(123)
        self.conn.execute.assert_any_call(text(
            "SELECT COUNT(*) AS late_pay_count FROM payments WHERE customer_id = :customer_id AND status = 'Late'"), 
            {"customer_id": 123})

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_commit_on_transaction(self, mock_engine):
        calculate_credit_score(123)
        self.conn.begin.return_value.commit.assert_called_once()

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_close_on_connection(self, mock_engine):
        calculate_credit_score(123)
        self.conn.close.assert_called_once()

    @patch('your_module.engine')
    def test_calculate_credit_score_calls_rollback_on_transaction_if_exception_occurs(self, mock_engine):
        self.conn.execute.side_effect = psycopg2.Error
        try:
            calculate_credit_score(123)
        except psycopg2.Error:
            self.conn.begin.return_value.rollback.assert_called_once()
            return
        self.fail("Did not catch psycopg2.Error")

    def test_calculate_credit_score_with_zero_loan_amount(self):
        self.cursor.first.return_value = MagicMock(total_loan_amount=0, total_repayment=0, outstanding_loan_balance=0)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(credit_card_balance=1000)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(late_pay_count=2)
        credit_score = calculate_credit_score(123)
        self.assertEqual(credit_score, 550)  # 850 - 2 * 50 (penalty for late payments) - 300 + 50

    def test_calculate_credit_score_with_nonzero_loan_amount(self):
        self.cursor.first.return_value = MagicMock(total_loan_amount=10000, total_repayment=5000, outstanding_loan_balance=5000)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(credit_card_balance=1000)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(late_pay_count=2)
        credit_score = calculate_credit_score(123)
        self.assertEqual(credit_score, 550)  # 500 + 400 / 2 - 2 * 50 (penalty for late payments) - 300 + 50

    def test_calculate_credit_score_with_nonzero_credit_card_balance(self):
        self.cursor.first.return_value = MagicMock(total_loan_amount=0, total_repayment=0, outstanding_loan_balance=0)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(credit_card_balance=10000)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(late_pay_count=2)
        credit_score = calculate_credit_score(123)
        self.assertEqual(credit_score, 300)  # 850 - 2 * 50 (penalty for late payments) - 300 + 300 - 500

    def test_calculate_credit_score_with_multiple_late_payments(self):
        self.cursor.first.return_value = MagicMock(total_loan_amount=0, total_repayment=0, outstanding_loan_balance=0)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(credit_card_balance=0)
        self.cursor.reset_mock()
        self.cursor.first.return_value = MagicMock(late_pay_count=20)
        credit_score = calculate_credit_score(123)
        self.assertEqual(credit_score, 200)  # 850 - 20 * 50 (penalty for late payments) - 500

if __name__ == '__main__':
    unittest.main()
