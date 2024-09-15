
import unittest
from unittest.mock import patch, MagicMock
from your_module import update_credit_score

class TestUpdateCreditScore(unittest.TestCase):

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_all_values(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 1000,
            "total_repayment": 900,
            "outstanding_loan_balance": 100,
            "credit_card_balance": 300,
            "late_payment_count": 1
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 300}, {"late_payment_count": 1}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 659)

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_zero_loan_amount(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 0,
            "total_repayment": 0,
            "outstanding_loan_balance": 0,
            "credit_card_balance": 0,
            "late_payment_count": 0
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 0}, {"late_payment_count": 0}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 700)

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_zero_credit_card_balance(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 1000,
            "total_repayment": 900,
            "outstanding_loan_balance": 100,
            "credit_card_balance": 0,
            "late_payment_count": 1
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 0}, {"late_payment_count": 1}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 649)

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_max_late_payments(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 1000,
            "total_repayment": 900,
            "outstanding_loan_balance": 100,
            "credit_card_balance": 300,
            "late_payment_count": 18
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 300}, {"late_payment_count": 18}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 300)

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_min_late_payments(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 1000,
            "total_repayment": 900,
            "outstanding_loan_balance": 100,
            "credit_card_balance": 300,
            "late_payment_count": 0
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 300}, {"late_payment_count": 0}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 649)

    @patch('your_module.engine.connect')
    def test_update_credit_score_with_min_score(self, mock_engine):
        connection = mock_engine().__enter__()
        connection.execute = MagicMock(return_value=None)
        fetchone_result = {
            "total_loan_amount": 1000,
            "total_repayment": 0,
            "outstanding_loan_balance": 100,
            "credit_card_balance": 10000,
            "late_payment_count": 15
        }
        connection.execute.return_value.fetchone.side_effect = [fetchone_result, fetchone_result, {"credit_card_balance": 10000}, {"late_payment_count": 15}]
        customer_id = 12345
        updated_score = update_credit_score(customer_id)
        self.assertEqual(updated_score, 300)

if __name__ == '__main__':
    unittest.main()
