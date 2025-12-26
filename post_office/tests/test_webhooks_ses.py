import json
from datetime import datetime, timezone

from django.test import RequestFactory, TestCase, override_settings

from post_office.models import RecipientDeliveryStatus
from post_office.webhooks.ses import SESWebhookHandler


class SESWebhookHandlerTest(TestCase):
    """Tests for SESWebhookHandler.parse_events()."""

    def setUp(self):
        self.factory = RequestFactory()
        self.handler = SESWebhookHandler()

    def _make_sns_payload(self, message):
        """Helper to wrap a message in SNS notification format."""
        return {
            'Type': 'Notification',
            'MessageId': 'test-message-id',
            'Message': json.dumps(message),
        }

    def _make_request(self, payload):
        """Helper to create a POST request with JSON payload."""
        return self.factory.post(
            '/webhook/ses/',
            data=json.dumps(payload),
            content_type='application/json',
        )

    def test_parse_delivery_multiple_recipients(self):
        """Test parsing a SES delivery with multiple recipients."""
        expected_timestamp = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        message = {
            'notificationType': 'Delivery',
            'mail': {
                'messageId': 'abc123',
                'destination': ['user1@example.com', 'user2@example.com', 'user3@example.com'],
                'timestamp': '2024-01-02T03:04:05Z',
            },
            'delivery': {
                'recipients': ['user1@example.com', 'user2@example.com'],
            },
        }
        payload = self._make_sns_payload(message)
        request = self._make_request(payload)
        events = self.handler.parse_events(request)

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].recipient, 'user1@example.com')
        self.assertEqual(events[1].recipient, 'user2@example.com')
        self.assertEqual(events[0].timestamp, expected_timestamp)
        self.assertEqual(events[1].timestamp, expected_timestamp)
        self.assertEqual(
            events[0].to_addresses,
            ['user1@example.com', 'user2@example.com', 'user3@example.com'],
        )
        self.assertEqual(
            events[1].to_addresses,
            ['user1@example.com', 'user2@example.com', 'user3@example.com'],
        )

    def test_parse_bounce_permanent(self):
        """Test parsing a SES permanent bounce."""
        message = {
            'notificationType': 'Bounce',
            'mail': {
                'messageId': 'abc123',
                'destination': ['invalid@example.com'],
            },
            'bounce': {
                'bounceType': 'Permanent',
                'bouncedRecipients': [
                    {'emailAddress': 'invalid@example.com'},
                ],
            },
        }
        payload = self._make_sns_payload(message)
        request = self._make_request(payload)
        events = self.handler.parse_events(request)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].raw_event, 'Bounce:Permanent')
        self.assertEqual(events[0].delivery_status, RecipientDeliveryStatus.HARD_BOUNCED)
        self.assertEqual(events[0].recipient, 'invalid@example.com')

    def test_parse_bounce_transient(self):
        """Test parsing a SES transient (soft) bounce."""
        message = {
            'notificationType': 'Bounce',
            'mail': {
                'messageId': 'abc123',
                'destination': ['user@example.com'],
            },
            'bounce': {
                'bounceType': 'Transient',
                'bouncedRecipients': [
                    {'emailAddress': 'user@example.com'},
                ],
            },
        }
        payload = self._make_sns_payload(message)
        request = self._make_request(payload)
        events = self.handler.parse_events(request)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].delivery_status, RecipientDeliveryStatus.SOFT_BOUNCED)

    def test_parse_complaint(self):
        """Test parsing a SES complaint notification."""
        message = {
            'notificationType': 'Complaint',
            'mail': {
                'messageId': 'abc123',
                'destination': ['user@example.com'],
            },
            'complaint': {
                'complainedRecipients': [
                    {'emailAddress': 'user@example.com'},
                ],
            },
        }
        payload = self._make_sns_payload(message)
        request = self._make_request(payload)
        events = self.handler.parse_events(request)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].raw_event, 'Complaint')
        self.assertEqual(events[0].delivery_status, RecipientDeliveryStatus.SPAM_COMPLAINT)

    @override_settings(POST_OFFICE={'WEBHOOKS': {'SES': {'VERIFY_SIGNATURE': False}}})
    def test_parse_subscription_confirmation(self):
        """Test handling SNS subscription confirmation."""
        payload = {
            'Type': 'SubscriptionConfirmation',
            'TopicArn': 'arn:aws:sns:us-east-1:123456789:ses-notifications',
            'SubscribeURL': 'https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription',
        }
        request = self._make_request(payload)
        response = self.handler.post(request)

        self.assertEqual(response.status_code, 200)
        response_data = json.loads(response.content)
        self.assertEqual(response_data['status'], 'subscription_confirmation_received')
        self.assertIn('subscribe_url', response_data)

    def test_parse_empty_message(self):
        """Test parsing an empty SNS message."""
        payload = {
            'Type': 'Notification',
            'Message': '',
        }
        request = self._make_request(payload)
        events = self.handler.parse_events(request)
        self.assertEqual(len(events), 0)

    def test_parse_unknown_notification_type(self):
        """Test that unknown notification types are ignored."""
        message = {
            'notificationType': 'UnknownType',
            'mail': {
                'messageId': 'abc123',
                'destination': ['user@example.com'],
            },
        }
        payload = self._make_sns_payload(message)
        request = self._make_request(payload)
        events = self.handler.parse_events(request)
        self.assertEqual(len(events), 0)
