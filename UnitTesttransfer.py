
import unittest
import pytest
from unittest.mock import patch, Mock
from sqlalchemy.engine import mock
from sqlalchemy.exc import SQLAlchemyError
from your_module import transfer_balance  # replace 'your_module' with actual module name

class TestTransferBalance(unittest.TestCase):

    @patch('your_module.engine.connect')
    @patch('your_module.text')
    def test_transfer_balance_success(self, mock_text, mock_connect):
        sender_id = 1
        receiver_id = 2
        amount = 100.0

        mock_connection = mock.Connectable()
        mock_connection.return_value.close.return_value = None
        mock_connection.return_value.commit.return_value = None
        mock_connect.return_value = mock_connection.return_value

        mock_execute = mock_connection.return_value.execute.return_value
        mock_execute.return_value = None

        transfer_balance(sender_id, receiver_id, amount)

        mock_text.assert_called()
        mock_connect.assert_called()
        mock_connection.return_value.execute.assert_any_call(mock_text(), {"sender_id": sender_id, "amount": amount})
        mock_connection.return_value.execute.assert_any_call(mock_text(), {"receiver_id": receiver_id, "amount": amount})
        mock_connection.return_value.commit.assert_called()

    @patch('your_module.engine.connect')
    @patch('your_module.text')
    def test_transfer_balance_psycopg2_error(self, mock_text, mock_connect):
        sender_id = 1
        receiver_id = 2
        amount = 100.0

        mock_connection = mock.Connectable()
        mock_connection.return_value.close.return_value = None
        mock_connection.return_value.rollback.return_value = None
        mock_connect.return_value = mock_connection.return_value

        mock_execute = mock_connection.return_value.execute.return_value
        mock_execute.side_effect = [None, mock.Mock(side_effect=Exception("SQL Error"))]

        with pytest.raises(Exception) as ex:
            transfer_balance(sender_id, receiver_id, amount)
        assert "Error transferring balance: SQL Error" in str(ex.value)

        mock_connect.assert_called()
        mock_connection.return_value.execute.assert_any_call(mock_text(), {"sender_id": sender_id, "amount": amount})
        mock_connection.return_value.execute.assert_any_call(mock_text(), {"receiver_id": receiver_id, "amount": amount})
        mock_connection.return_value.rollback.assert_called()
        mock_connection.return_value.close.assert_called()

    @patch('your_module.engine.connect')
    @patch('your_module.text')
    def test_transfer_balance_psycopg2_error_update_sender(self, mock_text, mock_connect):
        sender_id = 1
        receiver_id = 2
        amount = 100.0

        mock_connection = mock.Connectable()
        mock_connection.return_value.close.return_value = None
        mock_connection.return_value.rollback.return_value = None
        mock_connect.return_value = mock_connection.return_value

        mock_execute = mock_connection.return_value.execute.return_value
        mock_execute.side_effect = [mock.Mock(side_effect=Exception("SQL Error")), None]

        with pytest.raises(Exception) as ex:
            transfer_balance(sender_id, receiver_id, amount)
        assert "Error transferring balance: SQL Error" in str(ex.value)

        mock_connect.assert_called()
        mock_connection.return_value.execute.assert_any_call(mock_text(), {"sender_id": sender_id, "amount": amount})
        mock_connection.return_value.rollback.assert_called()
        mock_connection.return_value.close.assert_called()

    @patch('your_module.engine.connect')
    @patch('your_module.text')
    def test_transfer_balance_sqlalchemy_error(self, mock_text, mock_connect):
        sender_id = 1
        receiver_id = 2
        amount = 100.0

        mock_connection = mock.Connectable()
        mock_connection.return_value.close.return_value = None
        mock_connect.return_value = mock_connection.return_value

        mock_execute = mock_connection.return_value.execute.return_value
        mock_execute.side_effect = [SQLAlchemyError]

        with pytest.raises(Exception) as ex:
            transfer_balance(sender_id, receiver_id, amount)
        assert "Error transferring balance: " in str(ex.value)

        mock_text.assert_called()
        mock_connect.assert_called()
        mock_connection.return_value.execute.assert_called_once()

    def test_transfer_balance_required_args(self):
        with pytest.raises(TypeError):
            transfer_balance(1, 2)

        with pytest.raises(TypeError):
            transfer_balance(1, 2, 100.0, None)
