import uuid
from django.db import models

class Domain(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    domain_name = models.CharField(blank=False, max_length=255, unique=True)
    is_threat = models.BooleanField(default=False)
    threatment_started_at = models.DateTimeField(blank=True, null=True)
    threatment_ended_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.domain