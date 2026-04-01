from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import CommandError, call_command
from django.test import TestCase
from openpyxl import Workbook

from parsers.models import Letters


class ImportLettersCommandTests(TestCase):
    def _create_xlsx(
        self,
        directory: str,
        rows: list[list[str]],
        headers: list[str] | None = None,
    ) -> Path:
        file_path = Path(directory) / "letters.xlsx"
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(headers or ["external_id", "user_id", "email", "subject", "message"])
        for row in rows:
            sheet.append(row)
        workbook.save(file_path)
        workbook.close()
        return file_path

    def test_import_creates_records_and_reports_stats(self):
        Letters.objects.create(
            external_id="exists-1",
            user_id="u-old",
            email="old@example.com",
            subject="old-subject",
            message="old-message",
        )
        with TemporaryDirectory() as tmp_dir:
            file_path = self._create_xlsx(
                tmp_dir,
                [
                    ["new-1", "u-1", "user1@example.com", "s1", "m1"],
                    ["new-1", "u-1-dup", "dup@example.com", "s1d", "m1d"],  # duplicate in file
                    ["exists-1", "u-2", "user2@example.com", "s2", "m2"],  # duplicate in DB
                    ["bad-1", "u-3", "not-an-email", "s3", "m3"],  # invalid email
                    ["new-2", "u-4", "user4@example.com", "s4", "m4"],
                ],
            )
            stdout = StringIO()
            call_command("import_letters", str(file_path), stdout=stdout)

        self.assertEqual(Letters.objects.count(), 3)
        self.assertTrue(Letters.objects.filter(external_id="new-1").exists())
        self.assertTrue(Letters.objects.filter(external_id="new-2").exists())
        output = stdout.getvalue()
        self.assertIn("Processed rows: 5", output)
        self.assertIn("Created records: 2", output)
        self.assertIn("Skipped records: 2", output)
        self.assertIn("Error rows: 1", output)

    def test_reimport_is_idempotent(self):
        with TemporaryDirectory() as tmp_dir:
            file_path = self._create_xlsx(
                tmp_dir,
                [
                    ["id-1", "u-1", "u1@example.com", "s1", "m1"],
                    ["id-2", "u-2", "u2@example.com", "s2", "m2"],
                ],
            )
            call_command("import_letters", str(file_path))
            call_command("import_letters", str(file_path))

        self.assertEqual(Letters.objects.count(), 2)

    def test_missing_required_columns_raises_error(self):
        with TemporaryDirectory() as tmp_dir:
            file_path = self._create_xlsx(
                tmp_dir,
                rows=[["id-1", "u-1", "u1@example.com"]],
                headers=["external_id", "user_id", "email"],
            )
            with self.assertRaises(CommandError):
                call_command("import_letters", str(file_path))


class SendEmailsCommandTests(TestCase):
    @patch("delivery.management.commands.send_emails.send_email")
    def test_sends_only_pending_records_and_marks_as_sent(self, send_email_mock):
        pending_1 = Letters.objects.create(
            external_id="ext-1",
            user_id="u-1",
            email="u1@example.com",
            subject="sub-1",
            message="msg-1",
            is_sent=False,
        )
        pending_2 = Letters.objects.create(
            external_id="ext-2",
            user_id="u-2",
            email="u2@example.com",
            subject="sub-2",
            message="msg-2",
            is_sent=False,
        )
        already_sent = Letters.objects.create(
            external_id="ext-3",
            user_id="u-3",
            email="u3@example.com",
            subject="sub-3",
            message="msg-3",
            is_sent=True,
        )

        stdout = StringIO()
        call_command("send_emails", stdout=stdout)

        pending_1.refresh_from_db()
        pending_2.refresh_from_db()
        already_sent.refresh_from_db()

        self.assertTrue(pending_1.is_sent)
        self.assertTrue(pending_2.is_sent)
        self.assertTrue(already_sent.is_sent)
        self.assertEqual(send_email_mock.call_count, 2)
        self.assertIn("sent_records': 2", stdout.getvalue())

    @patch("delivery.management.commands.send_emails.send_email")
    def test_send_emails_continues_after_error(self, send_email_mock):
        first = Letters.objects.create(
            external_id="ext-1",
            user_id="u-1",
            email="u1@example.com",
            subject="sub-1",
            message="msg-1",
            is_sent=False,
        )
        second = Letters.objects.create(
            external_id="ext-2",
            user_id="u-2",
            email="u2@example.com",
            subject="sub-2",
            message="msg-2",
            is_sent=False,
        )
        send_email_mock.side_effect = [RuntimeError("smtp failed"), None]

        stdout = StringIO()
        stderr = StringIO()
        call_command("send_emails", stdout=stdout, stderr=stderr)

        first.refresh_from_db()
        second.refresh_from_db()

        self.assertFalse(first.is_sent)
        self.assertTrue(second.is_sent)
        self.assertIn("error_rows': 1", stdout.getvalue())
        self.assertIn("smtp failed", stderr.getvalue())

    def test_send_emails_when_nothing_to_send(self):
        Letters.objects.create(
            external_id="ext-1",
            user_id="u-1",
            email="u1@example.com",
            subject="sub-1",
            message="msg-1",
            is_sent=True,
        )
        stdout = StringIO()
        call_command("send_emails", stdout=stdout)
        self.assertIn("No records to send", stdout.getvalue())
