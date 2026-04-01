# Email Delivery Task

Simple Django project for importing email records from an XLSX file and sending them (simulated).

## Requirements
- Python 3.10+
- Django 4.2+

## Setup
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
python manage.py migrate
```

## XLSX Format
First row must contain headers:
- `external_id`
- `user_id`
- `email`
- `subject`
- `message`

`external_id` is used to skip duplicates on repeated imports.

## Commands
Import records:
```bash
python manage.py import_letters path/to/file.xlsx
```

Send pending emails:
```bash
python manage.py send_emails
```

Email sending is simulated via logging with a random delay.

## Tests
```bash
python manage.py test delivery.tests
```
