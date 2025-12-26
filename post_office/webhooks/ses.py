from __future__ import annotations

import json
import logging
from datetime import datetime

from django.http import HttpRequest, HttpResponse, JsonResponse

from post_office.models import RecipientDeliveryStatus
from post_office.webhooks.base import BaseWebhookHandler, ESPEvent

logger = logging.getLogger(__name__)


class SESWebhookHandler(BaseWebhookHandler):
    """
    Webhook handler for AWS SES via SNS.

    SES sends notifications through SNS with the following structure:
    {
        "Type": "Notification",
        "MessageId": "...",
        "Message": "{\"notificationType\":\"Delivery\", ...}",  # JSON string
        ...
    }

    For subscription confirmation:
    {
        "Type": "SubscriptionConfirmation",
        "SubscribeURL": "https://...",
        ...
    }

    The inner Message contains:
    {
        "notificationType": "Delivery",  # or "Bounce", "Complaint"
        "mail": {
            "messageId": "...",
            "destination": ["recipient@example.com"],
            "commonHeaders": {
                "subject": "Test Subject"
            }
        },
        "delivery": {...},  # or "bounce": {...}, "complaint": {...}
    }
    """

    def verify_signature(self, request: HttpRequest) -> bool:
        """
        Verify AWS SNS message signature using X.509 certificate.

        TODO: Implement in Stage 4
        """
        # Placeholder - will be implemented in signature verification stage
        return True

    def post(self, request: HttpRequest) -> HttpResponse:
        """Handle incoming SNS POST request, including subscription confirmation."""
        if not self.verify_signature(request):
            logger.warning('SNS signature verification failed')
            return JsonResponse({'error': 'Invalid signature'}, status=401)

        try:
            payload = json.loads(request.body)
        except json.JSONDecodeError as e:
            logger.error(f'Failed to parse SNS payload: {e}')
            return JsonResponse({'error': 'Invalid JSON'}, status=400)

        message_type = payload.get('Type', '')

        # Handle subscription confirmation
        if message_type == 'SubscriptionConfirmation':
            return self._handle_subscription_confirmation(payload)

        # Handle unsubscribe confirmation
        if message_type == 'UnsubscribeConfirmation':
            logger.info('Received SNS UnsubscribeConfirmation')
            return JsonResponse({'status': 'ok'})

        # Handle normal notifications
        if message_type == 'Notification':
            return self._handle_notification(payload)

        logger.warning(f'Unknown SNS message type: {message_type}')
        return JsonResponse({'error': 'Unknown message type'}, status=400)

    def _handle_subscription_confirmation(self, payload: dict) -> HttpResponse:
        """
        Handle SNS subscription confirmation.

        In production, you should fetch the SubscribeURL to confirm.
        For security, this should be done manually or with proper validation.
        """
        subscribe_url = payload.get('SubscribeURL')
        topic_arn = payload.get('TopicArn')
        logger.info(f'SNS subscription confirmation received. TopicArn: {topic_arn}, SubscribeURL: {subscribe_url}')
        # Return 200 to acknowledge receipt
        # Actual confirmation should be done by fetching SubscribeURL
        return JsonResponse(
            {
                'status': 'subscription_confirmation_received',
                'topic_arn': topic_arn,
                'subscribe_url': subscribe_url,
            }
        )

    def _handle_notification(self, payload: dict) -> HttpResponse:
        """Handle SNS notification containing SES event."""
        try:
            events = self.parse_events(payload)
        except Exception as e:
            logger.exception(f'Error parsing SES events: {e}')
            return JsonResponse({'error': 'Failed to parse events'}, status=400)

        self.handle_events(events, payload)

        return JsonResponse({'status': 'ok', 'processed': len(events)})

    def parse_events(self, request: HttpRequest | dict) -> list[ESPEvent]:
        """Parse SES webhook payload into ESPEvent objects."""
        if isinstance(request, HttpRequest):
            try:
                payload = json.loads(request.body)
            except json.JSONDecodeError:
                logger.error('Failed to parse SES webhook JSON')
                raise
        else:
            payload = request

        events = []

        # Extract the inner Message (it's a JSON string)
        message_str = payload.get('Message', '')
        if not message_str:
            logger.warning('No Message in SNS payload')
            return events

        try:
            message = json.loads(message_str)
        except json.JSONDecodeError:
            logger.error('Failed to parse inner SNS Message JSON')
            return events

        notification_type = message.get('notificationType', '')
        mail_data = message.get('mail', {})

        # Get common mail info
        message_id = mail_data.get('messageId')
        destinations = mail_data.get('destination', [])
        common_headers = mail_data.get('commonHeaders', {})
        subject = common_headers.get('subject')

        # Parse timestamp
        timestamp = None
        if ts := mail_data.get('timestamp'):
            try:
                timestamp = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

        # Process based on notification type
        if notification_type == 'Delivery':
            delivery_data = message.get('delivery', {})
            recipients = delivery_data.get('recipients', destinations)
            for recipient in recipients:
                events.append(
                    ESPEvent(
                        raw_event='Delivery',
                        delivery_status=RecipientDeliveryStatus.DELIVERED,
                        recipient=recipient,
                        message_id=message_id,
                        timestamp=timestamp,
                        subject=subject,
                        to_addresses=destinations,
                    )
                )

        elif notification_type == 'Bounce':
            bounce_data = message.get('bounce', {})
            bounce_type = bounce_data.get('bounceType', 'Permanent')
            bounced_recipients = bounce_data.get('bouncedRecipients', [])

            if bounce_type == 'Transient':
                status = RecipientDeliveryStatus.SOFT_BOUNCED
            else:
                status = RecipientDeliveryStatus.HARD_BOUNCED

            for recipient_info in bounced_recipients:
                recipient = recipient_info.get('emailAddress', '')
                if recipient:
                    events.append(
                        ESPEvent(
                            raw_event=f'Bounce:{bounce_type}',
                            delivery_status=status,
                            recipient=recipient,
                            message_id=message_id,
                            timestamp=timestamp,
                            subject=subject,
                            to_addresses=destinations,
                        )
                    )

        elif notification_type == 'Complaint':
            complaint_data = message.get('complaint', {})
            complained_recipients = complaint_data.get('complainedRecipients', [])

            for recipient_info in complained_recipients:
                recipient = recipient_info.get('emailAddress', '')
                if recipient:
                    events.append(
                        ESPEvent(
                            raw_event='Complaint',
                            delivery_status=RecipientDeliveryStatus.SPAM_COMPLAINT,
                            recipient=recipient,
                            message_id=message_id,
                            timestamp=timestamp,
                            subject=subject,
                            to_addresses=destinations,
                        )
                    )

        else:
            logger.debug(f'Ignoring unmapped SES notification type: {notification_type}')

        return events
