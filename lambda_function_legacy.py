import json
import boto3
import uuid
import base64
from datetime import datetime
from decimal import Decimal
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from botocore.exceptions import ClientError
from io import BytesIO

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

clients_table = dynamodb.Table('Clients')
addresses_table = dynamodb.Table('Addresses')
products_table = dynamodb.Table('Products')
sales_notes_table = dynamodb.Table('SalesNotes')
sales_note_items_table = dynamodb.Table('SalesNoteItems')

BUCKET_NAME = '750924-esi3898k-examen1'

def lambda_handler(event, context):
    http_method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("routeKey", "")  
    body = json.loads(event.get('body', '{}')) if event.get('body') else {}

    try:
        if'/clients' in path:
            if http_method == 'POST':
                client_id = str(uuid.uuid4())
                clients_table.put_item(Item={
                    'ID': client_id,
                    'RazonSocial': body['RazonSocial'],
                    'NombreComercial': body['NombreComercial'],
                    'RFC': body['RFC'],
                    'CorreoElectronico': body['CorreoElectronico'],
                    'Telefono': body['Telefono']
                })
                return {'statusCode': 200, 'body': json.dumps({'ID': client_id})}
            
            elif http_method == 'GET':
                if 'ID' in event['queryStringParameters']:
                    response = clients_table.get_item(Key={'ID': event['queryStringParameters']['ID']})
                    return {'statusCode': 200, 'body': json.dumps(response.get('Item', {}))}
                else:
                    response = clients_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(response['Items'])}
            
            elif http_method == 'PUT':
                clients_table.update_item(
                    Key={'ID': body['ID']},
                    UpdateExpression='SET RazonSocial = :rs, NombreComercial = :nc, RFC = :rfc, CorreoElectronico = :ce, Telefono = :tel',
                    ExpressionAttributeValues={
                        ':rs': body['RazonSocial'],
                        ':nc': body['NombreComercial'],
                        ':rfc': body['RFC'],
                        ':ce': body['CorreoElectronico'],
                        ':tel': body['Telefono']
                    }
                )
                return {'statusCode': 200, 'body': json.dumps({'message': 'Client updated'})}
            
            elif http_method == 'DELETE':
                clients_table.delete_item(Key={'ID': body['ID']})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Client deleted'})}

        elif '/addresses' in path:
            if http_method == 'POST':
                address_id = str(uuid.uuid4())
                addresses_table.put_item(Item={
                    'ID': address_id,
                    'Domicilio': body['Domicilio'],
                    'Colonia': body['Colonia'],
                    'Municipio': body['Municipio'],
                    'Estado': body['Estado'],
                    'TipoDireccion': body['TipoDireccion']
                })
                return {'statusCode': 200, 'body': json.dumps({'ID': address_id})}
            
            elif http_method == 'GET':
                if 'ID' in event['queryStringParameters']:
                    response = addresses_table.get_item(Key={'ID': event['queryStringParameters']['ID']})
                    return {'statusCode': 200, 'body': json.dumps(response.get('Item', {}))}
                else:
                    response = addresses_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(response['Items'])}
            
            elif http_method == 'PUT':
                addresses_table.update_item(
                    Key={'ID': body['ID']},
                    UpdateExpression='SET Domicilio = :d, Colonia = :c, Municipio = :m, Estado = :e, TipoDireccion = :td',
                    ExpressionAttributeValues={
                        ':d': body['Domicilio'],
                        ':c': body['Colonia'],
                        ':m': body['Municipio'],
                        ':e': body['Estado'],
                        ':td': body['TipoDireccion']
                    }
                )
                return {'statusCode': 200, 'body': json.dumps({'message': 'Address updated'})}
            
            elif http_method == 'DELETE':
                addresses_table.delete_item(Key={'ID': body['ID']})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Address deleted'})}

        elif '/products' in path:
            if http_method == 'POST':
                product_id = str(uuid.uuid4())
                products_table.put_item(Item={
                    'ID': product_id,
                    'Nombre': body['Nombre'],
                    'UnidadMedida': body['UnidadMedida'],
                    'PrecioBase': Decimal(str(body['PrecioBase']))
                })
                return {'statusCode': 200, 'body': json.dumps({'ID': product_id})}
            
            elif http_method == 'GET':
                if 'ID' in event['queryStringParameters']:
                    response = products_table.get_item(Key={'ID': event['queryStringParameters']['ID']})
                    return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response.get('Item', {})))}
                else:
                    response = products_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response['Items']))}
            
            elif http_method == 'PUT':
                products_table.update_item(
                    Key={'ID': body['ID']},
                    UpdateExpression='SET Nombre = :n, UnidadMedida = :um, PrecioBase = :pb',
                    ExpressionAttributeValues={
                        ':n': body['Nombre'],
                        ':um': body['UnidadMedida'],
                        ':pb': Decimal(str(body['PrecioBase']))
                    }
                )
                return {'statusCode': 200, 'body': json.dumps({'message': 'Product updated'})}
            
            elif http_method == 'DELETE':
                products_table.delete_item(Key={'ID': body['ID']})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Product deleted'})}

        elif '/sales_notes' in path:
            if http_method == 'POST':
                note_id = str(uuid.uuid4())
                folio = str(uuid.uuid4())[:8]
                total = Decimal(str(body.get('Total', 0)))
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
                note_id = event['queryStringParameters']['ID']
                return {"statusCode" : 200, "body" : json.dumps(decimal_to_native(sales_notes_table.get_item(Key={'ID': note_id})))}
                note = sales_notes_table.get_item(Key={'ID': note_id})['Item']
                items = sales_note_items_table.scan(
                    FilterExpression='SalesNoteID = :snid',
                    ExpressionAttributeValues={':snid': note_id}
                )['Items']
                client = clients_table.get_item(Key={'ID': note['ClienteID']})['Item']
                response = {
                    'Note': note,
                    'Items': items,
                    'Client': client
                }
                return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response))}

        elif '/sales_note_items' in path:
            if http_method == 'POST':
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
                try:
                    existing_obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                    existing_metadata = existing_obj.get('Metadata', {})
                    veces_enviado = int(existing_metadata.get('veces-enviado', '0')) + 1
                except s3.exceptions.ClientError as e:
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

                sns = boto3.client('sns')
                s3_link = f'https://eg7ceanvbe.execute-api.us-east-1.amazonaws.com/pdf_note?ID={note_id}'
                message = {
                    'message': 'Nueva nota de venta creada o actualizada',
                    'client': client['RazonSocial'],
                    'folio': note['Folio'],
                    's3_link': s3_link
                }
                sns.publish(
                    TopicArn='arn:aws:sns:us-east-1:470813633828:Notas',
                    Message=json.dumps(message),
                    MessageGroupId=note['Folio']
                )

                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'PDF actualizado y enviado {veces_enviado} veces'})
                }

        elif '/pdf_note' in path and http_method == 'GET':
            note_id = event['queryStringParameters']['ID']
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