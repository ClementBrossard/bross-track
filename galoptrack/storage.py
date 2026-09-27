"""Stockage des données : dossier local (tests, Colab) ou Cloudflare R2
(API compatible S3) en production.

Configuration par variables d'environnement :
  GALOPTRACK_STORAGE = "r2" | "local"   (défaut : r2 si R2_BUCKET est défini)
  GALOPTRACK_LOCAL_DIR                  (défaut : ./galoptrack_data)
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
"""

import os
from pathlib import Path


class LocalStorage:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def __repr__(self):
        return f"LocalStorage({self.root})"

    def _p(self, key):
        return self.root / key

    def exists(self, key):
        return self._p(key).exists()

    def read_bytes(self, key):
        return self._p(key).read_bytes()

    def write_bytes(self, key, data, content_type=None):
        p = self._p(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)

    def list(self, prefix=""):
        base = self.root
        return sorted(
            str(p.relative_to(base)).replace(os.sep, "/")
            for p in base.rglob("*")
            if p.is_file() and str(p.relative_to(base)).replace(os.sep, "/").startswith(prefix)
        )


class R2Storage:
    def __init__(self, bucket, account_id, access_key_id, secret_access_key):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    def __repr__(self):
        return f"R2Storage({self.bucket})"

    def exists(self, key):
        from botocore.exceptions import ClientError

        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def read_bytes(self, key):
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def write_bytes(self, key, data, content_type=None):
        extra = {"ContentType": content_type} if content_type else {}
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)

    def list(self, prefix=""):
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return sorted(keys)


def get_storage(kind=None):
    kind = kind or os.environ.get("GALOPTRACK_STORAGE") or ("r2" if os.environ.get("R2_BUCKET") else "local")
    if kind == "local":
        return LocalStorage(os.environ.get("GALOPTRACK_LOCAL_DIR", "galoptrack_data"))
    if kind == "r2":
        missing = [v for v in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
                   if not os.environ.get(v)]
        if missing:
            raise RuntimeError(f"Stockage R2 : variables d'environnement manquantes : {', '.join(missing)}")
        return R2Storage(
            os.environ["R2_BUCKET"], os.environ["R2_ACCOUNT_ID"],
            os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"],
        )
    raise ValueError(f"Stockage inconnu : {kind}")
