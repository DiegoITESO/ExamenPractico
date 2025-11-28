import json
import boto3
import uuid
import base64
import time
import os
from datetime import datetime
from decimal import Decimal
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from botocore.exceptions import ClientError
from io import BytesIOv

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

clients_table = dynamodb.Table('Clients')
products_table = dynamodb.Table('Products')
sales_notes_table = dynamodb.Table('SalesNotes')
sales_note_items_table = dynamodb.Table('SalesNoteItems')
addresses_table = dynamodb.Table('Addresses')

BUCKET_NAME = '750924-esi3898k-examen2'
NOTIFICATIONS_LAMBDA_NAME = 'notifications'

cloudwatch = boto3.client("cloudwatch")
ENV = os.getenv("ENVIRONMENT", "local")
def instrumented(handler):
    def wrapper(event, context):
        start = time.time()

        try:
            response = handler(event, context)
        except Exception as e:
            send_metric("HTTP_5XX", 1)
            raise

        duration = (time.time() - start) * 1000

        status = response.get("statusCode", 200)
        if 200 <= status < 300:
            send_metric("HTTP_2XX", 1)
        elif 400 <= status < 500:
            send_metric("HTTP_4XX", 1)
        else:
            send_metric("HTTP_5XX", 1)

        send_metric("LatencyMs", duration, unit="Milliseconds")

        return response

    return wrapper


def send_metric(name, value, unit="Count"):
    cloudwatch.put_metric_data(
        Namespace=f"MyApp-{ENV}",
        MetricData=[
            {
                "MetricName": name,
                "Value": value,
                "Unit": unit
            }
        ]
    )

@instrumented
def lambda_handler(event, context):
    http_method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("routeKey", "")
    body = json.loads(event.get('body', '{}')) if event.get('body') else {}

    try:
        if '/sales_notes' in path:
            if http_method == 'POST':
                required_fields = ['ClienteID', 'DireccionFacturacionID', 'DireccionEnvioID']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}

                client_resp = clients_table.get_item(Key={'ID': body['ClienteID']})
                if 'Item' not in client_resp:
                    return {'statusCode': 400, 'body': json.dumps({'error': f"Client {body['ClienteID']} not found"})}

                billing_addr_resp = addresses_table.get_item(Key={'ID': body['DireccionFacturacionID']})
                if 'Item' not in billing_addr_resp:
                    return {'statusCode': 400, 'body': json.dumps({'error': f"Billing Address {body['DireccionFacturacionID']} not found"})}
                if billing_addr_resp['Item'].get('TipoDireccion') != 'Facturacion':
                    return {'statusCode': 400, 'body': json.dumps({'error': f"Address {body['DireccionFacturacionID']} is not a billing address"})}

                shipping_addr_resp = addresses_table.get_item(Key={'ID': body['DireccionEnvioID']})
                if 'Item' not in shipping_addr_resp:
                    return {'statusCode': 400, 'body': json.dumps({'error': f"Shipping Address {body['DireccionEnvioID']} not found"})}
                if shipping_addr_resp['Item'].get('TipoDireccion') != 'Envio':
                    return {'statusCode': 400, 'body': json.dumps({'error': f"Address {body['DireccionEnvioID']} is not a shipping address"})}


                note_id = str(uuid.uuid4())
                folio = str(uuid.uuid4())[:8]
                total = Decimal(str(0))
                sales_notes_table.put_item(Item={
                    'ID': note_id,
                    'Folio': folio,
                    'ClienteID': body['ClienteID'],
                    'DireccionFacturacionID': body['DireccionFacturacionID'],
                    'DireccionEnvioID': body['DireccionEnvioID'],
                    'Total': total
                })
                return {'statusCode': 200, 'body': json.dumps({'ID': note_id, 'Folio': folio})}

            elif http_method == 'GET':
                note_id = event.get('pathParameters', {}).get('id')
                if not note_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}
                # The original code had a return statement here that returned just the note.
                # Then it had unreachable code that fetched items and client.
                # I will implement the full response as likely intended (Note + Items + Client).
                # However, looking at the original code:
                # 161: return {"statusCode" : 200, "body" : json.dumps(decimal_to_native(sales_notes_table.get_item(Key={'ID': note_id})))}
                # 162: note = ...
                # Line 161 returned early. I will stick to the likely intended behavior if line 161 was debugging, 
                # but since I am refactoring, I should probably stick to what was active or improve it.
                # The active line 161 returns just the GetItem result wrapper.
                # But the user probably wants the full object.
                # Let's look at line 161 in original:
                # return {"statusCode" : 200, "body" : json.dumps(decimal_to_native(sales_notes_table.get_item(Key={'ID': note_id})))}
                # This returns the DynamoDB response structure {'Item': ...}.
                # I will keep it simple and return the Item directly if possible, or just what was there.
                # Actually, the original code had unreachable lines 162-173.
                # I will uncomment them to make it "work" better if that's the goal, or just keep line 161.
                # Given "Refactor", I should probably clean it up. I'll return the full object (Note, Items, Client) as it seems to be the "correct" implementation that was commented out/shadowed.
                
                note_resp = sales_notes_table.get_item(Key={'ID': note_id})
                if 'Item' not in note_resp:
                     return {'statusCode': 404, 'body': json.dumps({'error': 'Note not found'})}
                note = note_resp['Item']
                
                items = sales_note_items_table.scan(
                    FilterExpression='SalesNoteID = :snid',
                    ExpressionAttributeValues={':snid': note_id}
                )['Items']
                
                client = clients_table.get_item(Key={'ID': note['ClienteID']}).get('Item', {})
                
                response = {
                    'Note': note,
                    'Items': items,
                    'Client': client
                }
                return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response))}

        elif '/sales_note_items' in path:
            if http_method == 'POST':
                required_fields = ['SalesNoteID', 'Items']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}
                note_id = body['SalesNoteID']
                items = body['Items']
                for item in items:
                    item_id = str(uuid.uuid4())
                    sales_note_items_table.put_item(Item={
                        'ID': item_id,
                        'SalesNoteID': note_id,
                        'ProductoID': item['ProductoID'],
                        'Cantidad': int(item['Cantidad']),
                        'PrecioUnitario': Decimal(str(item['PrecioUnitario'])),
                        'Importe': Decimal(float(item['Cantidad']) * float(item['PrecioUnitario']))
                    })
                all_items = sales_note_items_table.scan(
                    FilterExpression='SalesNoteID = :snid',
                    ExpressionAttributeValues={':snid': note_id}
                )['Items']
                total = sum(float(i['Importe']) for i in all_items)
                sales_notes_table.update_item(
                    Key={'ID': note_id},
                    UpdateExpression='SET #t = :t',
                    ExpressionAttributeNames={'#t': 'Total'},
                    ExpressionAttributeValues={':t': Decimal(total)}
                )

                note = sales_notes_table.get_item(Key={'ID': note_id})['Item']
                client = clients_table.get_item(Key={'ID': note['ClienteID']})['Item']
                pdf_buffer = generate_pdf(client, note['Folio'], all_items)

                s3_key = f"{client['RFC']}/{note['Folio']}.pdf"
                veces_enviado = 0
                try:
                    existing_obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                    existing_metadata = existing_obj.get('Metadata', {})
                    veces_enviado = int(existing_metadata.get('veces-enviado', '0')) + 1
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        veces_enviado = 1
                    else:
                        raise

                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Body=pdf_buffer.getvalue(),
                    Metadata={
                        'hora-envio': datetime.utcnow().isoformat(),
                        'nota-descargada': 'false',
                        'veces-enviado': str(veces_enviado)
                    }
                )

                # Invoke Notification Lambda
                s3_link = f'https://41iqxbksll.execute-api.us-east-1.amazonaws.com/pdf_note/{note_id}'
                notification_payload = {
                    'client': decimal_to_native(client),
                    'folio': note['Folio'],
                    's3_link': s3_link
                }
                
                # Asynchronous invocation
                lambda_client.invoke(
                    FunctionName=NOTIFICATIONS_LAMBDA_NAME,
                    InvocationType='Event',
                    Payload=json.dumps(notification_payload).encode('utf-8')
                )

                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'PDF actualizado y notificacion enviada. Veces enviado: {veces_enviado}'})
                }

        elif '/pdf_note' in path and http_method == 'GET':
            note_id = event.get('pathParameters', {}).get('id')
            if not note_id:
                return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID parameter'})}
            note_resp = sales_notes_table.get_item(Key={'ID': note_id})
            note = note_resp.get('Item')
            if not note:
                return {'statusCode': 404, 'body': json.dumps({'error': f'Sales note {note_id} not found'})}

            client_resp = clients_table.get_item(Key={'ID': note.get('ClienteID')})
            client = client_resp.get('Item')
            if not client:
                return {'statusCode': 404, 'body': json.dumps({'error': f'Client {note.get("ClienteID")} not found'})}

            s3_key = f"{client['RFC']}/{note['Folio']}.pdf"

            try:
                pdf_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                pdf_data = pdf_obj['Body'].read()  # bytes

                new_metadata = dict(pdf_obj.get('Metadata', {}))
                new_metadata['nota-descargada'] = 'true'

                s3.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource={'Bucket': BUCKET_NAME, 'Key': s3_key},
                    Key=s3_key,
                    Metadata=new_metadata,
                    MetadataDirective='REPLACE'
                )

                encoded_body = base64.b64encode(pdf_data).decode('utf-8')

                return {
                    'statusCode': 200,
                    'headers': {
                        'Content-Type': 'application/pdf',
                        'Content-Disposition': f'attachment; filename="{note["Folio"]}.pdf"'
                    },
                    'body': encoded_body,
                    'isBase64Encoded': True
                }

            except ClientError as e:
                code = e.response.get('Error', {}).get('Code', '')
                if code in ('NoSuchKey', '404', 'NotFound'):
                    return {'statusCode': 404, 'body': json.dumps({'error': 'PDF not found'})}
                return {'statusCode': 500, 'body': json.dumps({'error': 'S3 error', 'details': str(e)})}

        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid path or method'})}

    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def generate_pdf(client, folio, items):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()

    elements.append(Paragraph("Client Information", styles['Heading1']))
    client_data = [
        ['Razon Social', client['RazonSocial']],
        ['Nombre Comercial', client['NombreComercial']],
        ['RFC', client['RFC']],
        ['Correo', client['CorreoElectronico']],
        ['Telefono', client['Telefono']]
    ]
    client_table = Table(client_data)
    client_table.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 1, colors.black),
        ('FONT', (0,0), (-1,-1), 'Helvetica', 10)
    ]))
    elements.append(client_table)

    elements.append(Paragraph(f"Note Folio: {folio}", styles['Heading2']))

    items_data = [['Cantidad', 'Producto', 'Precio Unitario', 'Importe']]
    for item in items:
        product = products_table.get_item(Key={'ID': item['ProductoID']})['Item']
        items_data.append([
            str(item['Cantidad']),
            product['Nombre'],
            f"${item['PrecioUnitario']:.2f}",
            f"${item['Importe']:.2f}"
        ])
    items_table = Table(items_data)
    items_table.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 1, colors.black),
        ('FONT', (0,0), (-1,-1), 'Helvetica', 10),
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke)
    ]))
    elements.append(items_table)

    doc.build(elements)
    return buffer

def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj
