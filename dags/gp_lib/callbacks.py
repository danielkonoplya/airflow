"""Колбэки алертинга. Webhook задаётся Airflow Variable gp_alert_webhook_url.

Тело запроса — {"text": "..."}: подходит для Mattermost/Slack incoming webhook
и легко адаптируется под Telegram-бота или корпоративный шлюз.
"""

from __future__ import annotations

import logging

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

ALERT_WEBHOOK_VARIABLE = "gp_alert_webhook_url"


def notify_failure(context) -> None:
    ti = context["task_instance"]
    text = (
        f"[Airflow] {ti.dag_id}.{ti.task_id} упал "
        f"(run {context.get('run_id')}, попытка {ti.try_number}): {context.get('exception')}\n{ti.log_url}"
    )
    log.error(text)

    url = Variable.get(ALERT_WEBHOOK_VARIABLE, default_var=None)
    if not url:
        return
    try:
        requests.post(url, json={"text": text}, timeout=10).raise_for_status()
    except requests.RequestException:
        log.exception("Не удалось отправить алерт в webhook")
