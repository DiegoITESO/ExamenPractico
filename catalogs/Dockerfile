FROM public.ecr.aws/lambda/python:3.13

COPY requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY . ${LAMBDA_TASK_ROOT}

CMD ["catalogs_lambda.lambda_handler"]
