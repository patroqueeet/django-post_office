from django.urls import path

from post_office.webhooks.ses import SESWebhookHandler


app_name = 'post_office_webhooks'

urlpatterns = [
    path('ses/', SESWebhookHandler.as_view(), name='ses'),
]
