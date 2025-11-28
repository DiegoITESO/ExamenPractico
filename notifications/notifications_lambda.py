import json
import boto3

sns = boto3.client('sns')
TOPIC_ARN = 'arn:aws:sns:us-east-1:470813633828:Notas'

def lambda_handler(event, context):
    """
    Handler for sending email notifications.
    Expected event payload:
    {
        "client": { "RazonSocial": "..." },
        "folio": "...",
        "s3_link": "..."
    }
    """
    try:
        # If invoked asynchronously, the event is the payload.
        # If invoked via API Gateway, the payload is in body.
        # Assuming direct invocation or payload is the event itself for simplicity in internal calls.
        payload = event
        if 'body' in event:
             try:
                 payload = json.loads(event['body'])
             except:
                 pass

        client_name = payload.get('client', {}).get('RazonSocial', 'Unknown Client')
        folio = payload.get('folio', 'Unknown Folio')
        s3_link = payload.get('s3_link', '')

        message = {
            'message': 'Nueva nota de venta creada o actualizada',
            'client': client_name,
            'folio': folio,
            's3_link': s3_link
        }

        response = sns.publish(
            TopicArn=TOPIC_ARN,
            Message=json.dumps(message),
            MessageGroupId=folio
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Notification sent', 'messageId': response.get('MessageId')})
        }

    except Exception as e:
        print(f"Error sending notification: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
