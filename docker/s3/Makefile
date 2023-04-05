all:

# Generate keys for S3 plugin
rsa-keys:
	openssl genrsa -out private.pem 512
	openssl rsa -in private.pem -outform PEM -out public.pem -pubout
