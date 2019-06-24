import os

logfile = os.environ.get('LOG_FILE', "dagflow.log")
log_level = os.environ.get("LOG_LEVEL", "DEBUG")

# [%d{yyyy-MM-dd HH:mm:ss.SSS}] [%thread] [%-5level]
# [%-40.36logger{40}:%-4.4line] - %msg%n

LoggingConfig = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "[%(asctime)s] [%(processName)s-%(process)d] [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s ",
            'datefmt': "%Y-%m-%d %H:%M:%S.0"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            # "stream": "ext://sys.stdout"
        },
        "file_handler": {
            "class": "logging.FileHandler",
            "level": "INFO",
            "formatter": "simple",
            "filename": logfile,
        },
    },
    "loggers": {
        "dagflow": {
            "level": log_level,
            "handlers": ["console", "file_handler"],
            "propagate": False
        }
    },
    "root": {
        "level": log_level,
        "handlers": ["console", "file_handler"]
    }
}