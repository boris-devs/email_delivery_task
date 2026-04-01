from django.db import models

class ParsedLetters(models.Model):
    external_id = models.CharField(max_length=255, unique=True)
    user_id = models.CharField(max_length=255)
    email = models.EmailField()
    subject = models.CharField(max_length=255)
    message = models.TextField()
