from django.core.management import BaseCommand

from delivery.services.email_sender import send_email
from parsers.models import Letters


class Command(BaseCommand):
    help = "Send emails for imported records"

    chunk_size = 5

    def handle(self, *args, **options):
        """
        Executes the email delivery logic for all pending records in the database.
        """
        stats = {"processed_rows": 0, "sent_records": 0, "error_rows": 0}

        queryset = Letters.objects.filter(is_sent=False)

        if not queryset.exists():
            self.stdout.write(self.style.SUCCESS("No records to send"))
            return

        letters_to_update = []

        for letter in queryset.iterator(chunk_size=self.chunk_size):
            stats["processed_rows"] += 1

            try:
                send_email(
                    user_id=letter.user_id,
                    email=letter.email,
                    subject=letter.subject,
                    message=letter.message,
                    external_id=letter.external_id,
                )

                letter.is_sent = True
                letters_to_update.append(letter)
                stats["sent_records"] += 1

            except Exception as exc:
                stats["error_rows"] += 1
                self.stderr.write(f"Error external_id={letter.external_id}: {exc}")

            if len(letters_to_update) >= self.chunk_size:
                Letters.objects.bulk_update(letters_to_update, ["is_sent"])
                letters_to_update = []

        if letters_to_update:
            Letters.objects.bulk_update(letters_to_update, ["is_sent"])

        self.stdout.write(self.style.SUCCESS(f"Finished. Stats: {stats}"))
