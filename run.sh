echo "run.sh"
echo "start python virtual environment"
source venv/Scripts/Activate
echo "run main.py to convert excel to accounts, and django json"
python main.py
echo "import data into django db"
cd accounts
python manage.py loaddata ../data/reporting_pack.json