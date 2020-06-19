from npm_common.base_exceptions import NPMError


class NPMException(Exception):
    pass


class LGRException(NPMException):
    pass


class LGReorgError(NPMError):
    pass


class CodePointOpenError(NPMError):
    pass
