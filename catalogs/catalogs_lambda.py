import json
import boto3
import uuid
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
clients_table = dynamodb.Table('Clients')
addresses_table = dynamodb.Table('Addresses')
products_table = dynamodb.Table('Products')

def lambda_handler(event, context):
    http_method = event.get("requestContext", {}).get("http", {}).get("method")
    path = event.get("routeKey", "")
    body = json.loads(event.get('body', '{}')) if event.get('body') else {}

    try:
        if '/clients' in path:
            if http_method == 'POST':
                required_fields = ['RazonSocial', 'NombreComercial', 'RFC', 'CorreoElectronico', 'Telefono']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}

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
                client_id = event.get('pathParameters', {}).get('id')
                if client_id:
                    response = clients_table.get_item(Key={'ID': client_id})
                    return {'statusCode': 200, 'body': json.dumps(response.get('Item', {}))}
                else:
                    response = clients_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(response['Items'])}
            
            elif http_method == 'PUT':
                client_id = event.get('pathParameters', {}).get('id')
                if not client_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}
                
                required_fields = ['RazonSocial', 'NombreComercial', 'RFC', 'CorreoElectronico', 'Telefono']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}

                clients_table.update_item(
                    Key={'ID': client_id},
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
                client_id = event.get('pathParameters', {}).get('id')
                if not client_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}
                clients_table.delete_item(Key={'ID': client_id})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Client deleted'})}

        elif '/addresses' in path:
            if http_method == 'POST':
                required_fields = ['Domicilio', 'Colonia', 'Municipio', 'Estado', 'TipoDireccion']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}
                if body['TipoDireccion'] != 'Facturacion' and body['TipoDireccion'] != 'Envio':
                    return {'statusCode': 400, 'body': 'Address type must be either Facturacion or Envio'}
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
                address_id = event.get('pathParameters', {}).get('id')
                if address_id:
                    response = addresses_table.get_item(Key={'ID': address_id})
                    return {'statusCode': 200, 'body': json.dumps(response.get('Item', {}))}
                else:
                    response = addresses_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(response['Items'])}
            
            elif http_method == 'PUT':
                address_id = event.get('pathParameters', {}).get('id')
                if not address_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}

                required_fields = ['Domicilio', 'Colonia', 'Municipio', 'Estado', 'TipoDireccion']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}
                if body['TipoDireccion'] != 'Facturacion' and body['TipoDireccion'] != 'Envio':
                    return {'statusCode': 400, 'body': 'Address type must be either Facturacion or Envio'}

                addresses_table.update_item(
                    Key={'ID': address_id},
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
                address_id = event.get('pathParameters', {}).get('id')
                if not address_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}
                addresses_table.delete_item(Key={'ID': address_id})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Address deleted'})}

        elif '/products' in path:
            if http_method == 'POST':
                required_fields = ['Nombre', 'UnidadMedida', 'PrecioBase']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}

                product_id = str(uuid.uuid4())
                products_table.put_item(Item={
                    'ID': product_id,
                    'Nombre': body['Nombre'],
                    'UnidadMedida': body['UnidadMedida'],
                    'PrecioBase': Decimal(str(body['PrecioBase']))
                })
                return {'statusCode': 200, 'body': json.dumps({'ID': product_id})}
            
            elif http_method == 'GET':
                product_id = event.get('pathParameters', {}).get('id')
                if product_id:
                    response = products_table.get_item(Key={'ID': product_id})
                    return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response.get('Item', {})))}
                else:
                    response = products_table.scan()
                    return {'statusCode': 200, 'body': json.dumps(decimal_to_native(response['Items']))}
            
            elif http_method == 'PUT':
                product_id = event.get('pathParameters', {}).get('id')
                if not product_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}

                required_fields = ['Nombre', 'UnidadMedida', 'PrecioBase']
                if not all(field in body for field in required_fields):
                    return {'statusCode': 400, 'body': json.dumps({'error': 'Missing required fields: ' + ', '.join([f for f in required_fields if f not in body])})}

                products_table.update_item(
                    Key={'ID': product_id},
                    UpdateExpression='SET Nombre = :n, UnidadMedida = :um, PrecioBase = :pb',
                    ExpressionAttributeValues={
                        ':n': body['Nombre'],
                        ':um': body['UnidadMedida'],
                        ':pb': Decimal(str(body['PrecioBase']))
                    }
                )
                return {'statusCode': 200, 'body': json.dumps({'message': 'Product updated'})}
            
            elif http_method == 'DELETE':
                product_id = event.get('pathParameters', {}).get('id')
                if not product_id:
                     return {'statusCode': 400, 'body': json.dumps({'error': 'Missing ID in path'})}
                products_table.delete_item(Key={'ID': product_id})
                return {'statusCode': 200, 'body': json.dumps({'message': 'Product deleted'})}

        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid path or method'})}

    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj
