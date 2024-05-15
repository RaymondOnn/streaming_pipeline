# Object Store

This module provides functionality related to interacting with MinIO.  
It includes functions for connecting to a MinIO server, uploading files, and downloading files.

### Functions:
* `get_minio_client`: Creates and returns a MinIO client based on the provided configuration.
* `get_bucket`: Creates bucket in MinIO if bucket not found.
* `put_json`: Jsonify a dict and write it as object to the bucket


::: src.flask_api.app.fx.object_store
    options:
        members_order: source