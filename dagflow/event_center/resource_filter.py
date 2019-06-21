from .base_request_filter import RequestFilterStatus


def check_resource(request):
    print(request)
    return RequestFilterStatus.PASS, ""


FILTERS = [check_resource]
