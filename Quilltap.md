# Quilltap

## Future

### Move to internal database, not Postgres

* S3 backend
  * Download files from S3, decrypt them
  * Work with them locally
  * Queue up the updates, encrypt them, and send them back
* Local file backend
  * Still encrypted whenever it reads or writes

### Multi-user model

* Use encryption to secure individual databases per user
  * Encrypted with two keys
    * One based on hash of user ID and something you get from the authentication provider
    * One based on one-time password set by user
  * Encrypt all files that are stored, particularly library files and images
