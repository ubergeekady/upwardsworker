## Upwards worker

### Setting Virtualenv and Dev environment on localhost

```
pip3 install virtualenv

mkdir upwardsworker
cd upwardsworker *
virtualenv venv -p python3
source venv/bin/activate *
git clone <repo_url>
cd upwardsworker *
pip install -r requirements.txt

python upwardsworker.py & (run it in ec2 instance background before closing) *
```
* (*)Follow the steps in ec2

### MySQL And AWS Credentials

Create a config.json file in the same folder like so -

```
{
	"testing": true,
	"aws_access_key_id":"",
	"aws_secret_access_key":"",
	"sqs_queue_url":"",
	"mysql_host":"",
	"mysql_user":"",
	"mysql_passwd":""
}
```

