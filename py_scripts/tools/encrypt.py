import grpc

class Encrypt:
    def __init__(self, tls_cert_path):
        self.tls_cert_path = tls_cert_path

    def get_tls_credentials(self):
        """Helper method to load TLS credentials"""
        with open(self.tls_cert_path, "rb") as f:
            trusted_certs = f.read()
        return grpc.ssl_channel_credentials(root_certificates=trusted_certs)

