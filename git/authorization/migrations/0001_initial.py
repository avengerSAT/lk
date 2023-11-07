# Generated by Django 4.2.3 on 2023-08-17 13:31

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='user_access',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.IntegerField()),
                ('access_lvl', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='user_urls',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('user_id', models.CharField(db_index=True, max_length=10)),
                ('user_urls', models.TextField()),
            ],
        ),
    ]