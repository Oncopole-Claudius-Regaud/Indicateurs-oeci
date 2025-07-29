import logging
import os

# Initialise un logger pour enregistrer les activités dans un fichier log


def configure_logger():
    log_dir = "/home/administrateur/airflow/logs"
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(log_dir, "etl_oncopole.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Logger configuré pour le DAG Airflow")
