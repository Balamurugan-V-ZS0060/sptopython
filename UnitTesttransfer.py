
import unittest
from unittest.mock import patch, Mock
from your_module import transfer_balance
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, DatabaseError
import pandas as pd
import psycopg2

class TestTransferBalance(unittest.TestCase):

    @patch('your_module.engine.connect')
    def test_transfer_balance_success(self, mock_engine_connect):
        mock_connection = Mock()
        mock_engine_connect.return_value.__enter__.return_value = mock_connection

        mock_execute = Mock(side_effect=[None, None])
        mock_connection.execute.side_effect = mock_execute

        transfer_balance(1, 2, 100.0)
        mock_execute.assert_any_call(text("UPDATE accounts SET balance = balance - :p_amount WHERE id = :p_sender;"), p_sender=1, p_amount=100.0)
        mock_execute.assert_any_call(text("UPDATE accounts SET balance = balance + :p_amount WHERE id = :p_receiver;"), p_receiver=2, p_amount=100.0)
        mock_connection.commit.assert_called_once()

    @patch('your_module.engine.connect')
    def test_transfer_balance_error(self, mock_engine_connect):
        mock_connection = Mock()
        mock_engine_connect.return_value.__enter__.return_value = mock_connection

        mock_execute = Mock(side_effect=[None, IntegrityError('', '', '')])
        mock_connection.execute.side_effect = mock_execute

        with self.assertRaises(IntegrityError):
            transfer_balance(1, 2, 100.0)

    @patch('your_module.engine.connect')
    def test_transfer_balance_database_error(self, mock_engine_connect):
        mock_connection = Mock()
        mock_engine_connect.return_value.__enter__.return_value = mock_connection

        mock_execute = Mock(side_effect=[None, DatabaseError('', '', '')])
        mock_connection.execute.side_effect = mock_execute

        with self.assertRaises(DatabaseError):
            transfer_balance(1, 2, 100.0)

    @patch('your_module.engine.connect')
    def test_transfer_balance_connection_close(self, mock_engine_connect):
        mock_connection = Mock()
        mock_engine_connect.return_value.__enter__.return_value = mock_connection

        mock_execute = Mock(side_effect=[None, None])
        mock_connection.execute.side_effect = mock_execute

        transfer_balance(1, 2, 100.0)
        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
