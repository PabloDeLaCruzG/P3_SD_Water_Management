# Genera certificado TLS autofirmado X.509v3 para WM_Registry y WM_Central

import argparse
import datetime
import ipaddress
import os

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def parse_args():
    parser = argparse.ArgumentParser(
        description="Genera server.crt y server.key autofirmados para Water Management"
    )
    parser.add_argument(
        "--ip",
        nargs="*",
        default=[],
        help="IP(s) publicas o privadas del servidor (ej. --ip 3.80.12.34). 127.0.0.1 se incluye siempre."
    )
    parser.add_argument(
        "--dns",
        nargs="*",
        default=[],
        help="Nombres DNS extra para incluir en el SAN (localhost, wm_registry y wm_central se incluyen siempre)."
    )
    parser.add_argument(
        "--out-dir",
        default="certs",
        help="Carpeta de salida para server.crt y server.key (por defecto: certs)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Dias de validez del certificado (por defecto: 365)"
    )
    return parser.parse_args()


def build_san_entries(extra_ips, extra_dns):
    # Evita duplicados manteniendo el orden, siempre incluye direcciones locales y de Docker
    raw_ips = ["127.0.0.1"] + list(extra_ips)
    raw_dns = ["localhost", "wm_registry", "wm_central"] + list(extra_dns)

    entries = []
    seen_ips = set()
    for ip_str in raw_ips:
        ip_obj = ipaddress.ip_address(ip_str)
        if ip_obj not in seen_ips:
            seen_ips.add(ip_obj)
            entries.append(x509.IPAddress(ip_obj))

    seen_dns = set()
    for dns_name in raw_dns:
        clean = dns_name.strip()
        if clean and clean not in seen_dns:
            seen_dns.add(clean)
            entries.append(x509.DNSName(clean))

    return entries


def generate_self_signed_cert(extra_ips, extra_dns, out_dir, days):
    # Par de claves RSA de 2048 bits (cifrado asimetrico para el handshake TLS)
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )
    public_key = private_key.public_key()

    # Subject e Issuer coinciden porque es un certificado autofirmado
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "ES"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "UA-SD-WaterManagement"),
        x509.NameAttribute(NameOID.COMMON_NAME, "WM-Server"),
    ])

    # Margen de 5 minutos atras por si hay un poco de desfase de reloj entre AWS EC2 y el laboratorio
    now = datetime.datetime.now(datetime.timezone.utc)
    san_entries = build_san_entries(extra_ips, extra_dns)

    # Extensiones X.509v3 requeridas por VERIFY_X509_STRICT en Python 3.12 / OpenSSL 3
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=days))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(public_key),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(public_key),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )

    os.makedirs(out_dir, exist_ok=True)
    cert_path = os.path.join(out_dir, "server.crt")
    key_path = os.path.join(out_dir, "server.key")

    with open(cert_path, "wb") as cert_file:
        cert_file.write(cert.public_bytes(serialization.Encoding.PEM))

    with open(key_path, "wb") as key_file:
        key_file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    # Permisos restrictivos en la clave privada (solo lectura/escritura del propietario)
    os.chmod(key_path, 0o600)

    print(f"[Cert] Certificado generado: {cert_path}")
    print(f"[Cert] Clave privada generada (0600): {key_path}")
    print(f"[Cert] SAN incluidos: {[str(entry.value) for entry in san_entries]}")


if __name__ == "__main__":
    args = parse_args()
    generate_self_signed_cert(args.ip, args.dns, args.out_dir, args.days)