FROM public.ecr.aws/lambda/python:3.13

# Copiar dependencias
COPY requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copiar el resto del código
COPY . ${LAMBDA_TASK_ROOT}

# Configurar el handler: handler.py -> handler(event, context)
CMD ["notifications_lambda.lambda_handler"]
