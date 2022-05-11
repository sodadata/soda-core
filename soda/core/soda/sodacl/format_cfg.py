from __future__ import annotations


class FormatHelper:
    @staticmethod
    def build_default_formats():
        # s stands for an optional set of spaces
        s = r" *"
        sign_any_opt = r"[-+]?"
        plus_opt = r"\+?"
        min = r"\-"
        digits = r"[0-9]+"
        decimal_point_comma = r"(\d+([\.,]\d+)?|([\.,]\d+))"
        decimal_point = r"(\d+(\.\d+)?|(\.\d+))"
        decimal_comma = r"(\d+(,\d+)?|(,\d+))"
        zero_integer = r"0+"
        zero_point_comma = r"(0+([\.,]0+)?|([\.,]0+))"
        zero_point = r"(0+(\.0+)?|(\.0+))"
        zero_comma = r"(0+(,0+)?|(,0+))"
        money_point = r"\d{1,3}(\,\d\d\d)*(.\d+)?"
        money_comma = r"\d{1,3}(\.\d\d\d)*(,\d+)?"
        currency = r"([A-Z]{3}|[a-z]{3})"

        day = r"([1-9]|0[1-9]|1[012])"
        month = r"([1-9]|[012][0-9]|3[01])"
        year = r"(19|20)?\d\d"
        hour24 = r"(0?[0-9]|[01]\d|2[0-3])"
        hour12 = r"(0?[0-9]|1[0-2])"
        minute = r"[0-5]?[0-9]"
        second = r"[0-5]?[0-9]([.,]\d+)?"
        year4 = r"(19|20)\d\d"
        month2 = r"(0[0-9]|1[12])"
        day2 = r"([012][0-9]|3[01])"
        hour2 = r"(0[0-9]|1[012])"
        minute2 = r"[0-5][0-9]"
        second2 = minute2

        return {
            "integer": f"^{s}{sign_any_opt}{s}{digits}{s}$",
            "positive integer": f"^{s}{plus_opt}{s}{digits}{s}$",
            "negative integer": f"^{s}({min}{s}{digits}|{zero_integer}){s}$",
            "decimal": f"^{s}{sign_any_opt}{s}{decimal_point_comma}{s}$",
            "positive decimal": f"^{s}{plus_opt}{s}{decimal_point_comma}{s}$",
            "negative decimal": f"^{s}({min}{s}{decimal_point_comma}|{zero_point_comma}){s}$",
            "decimal point": f"^{s}{sign_any_opt}{s}{decimal_point}{s}$",
            "positive decimal point": f"^{s}{plus_opt}{s}{decimal_point}{s}$",
            "negative decimal point": f"^{s}({min}{s}{decimal_point}|{zero_point}){s}$",
            "decimal comma": f"^{s}{sign_any_opt}{s}{decimal_comma}{s}$",
            "positive decimal comma": f"^{s}{plus_opt}{s}{decimal_comma}{s}$",
            "negative decimal comma": f"^{s}({min}{s}{decimal_comma}|{zero_comma}){s}$",
            "percentage": f"^{s}{sign_any_opt}{s}{decimal_point_comma}{s}%{s}$",
            "positive percentage": f"^{s}{plus_opt}{s}{decimal_point_comma}{s}%{s}$",
            "negative percentage": f"^{s}({min}{s}{decimal_point_comma}|{zero_point_comma}){s}%{s}$",
            "percentage point": f"^{s}{sign_any_opt}{s}{decimal_point}{s}%{s}$",
            "positive percentage point": f"^{s}{plus_opt}{s}{decimal_point}{s}%{s}$",
            "negative percentage point": f"^{s}({min}{s}{decimal_point}|{zero_point}){s}%{s}$",
            "percentage comma": f"^{s}{sign_any_opt}{s}{decimal_comma}{s}%{s}$",
            "positive percentage comma": f"^{s}{plus_opt}{s}{decimal_comma}{s}%{s}$",
            "negative percentage comma": f"^{s}({min}{s}{decimal_comma}|{zero_comma}){s}%{s}$",
            "money": f"^{s}{sign_any_opt}{s}{decimal_point_comma}{s}{currency}{s}$",
            "money point": f"^{s}{sign_any_opt}{s}{money_point}{s}{currency}{s}$",
            "money comma": f"^{s}{sign_any_opt}{s}{money_comma}{s}{currency}{s}$",
            "date us": rf"^{s}{month}[-\./]{day}[-\./]{year}{s}$",
            "date eu": rf"^{s}{day}[-\./]{month}[-\./]{year}{s}$",
            "date inverse": rf"^{s}{year}[-\./]{month}[-\./]{day}{s}$",
            "date iso 8601": f"^{s}"
            rf"{year4}-?({month2}-?{day2}|W[0-5]\d(-?[1-7])?|[0-3]\d\d)"
            rf"([ T]{hour2}(:?{minute2}(:?{second2}([.,]\d+)?)?)?([+-]{hour2}:?{minute2}|Z)?)?"
            f"{s}$",
            "time 24h": f"^{s}{hour24}:{minute}(:{second})?{s}$",
            "time 24h nosec": f"^{s}{hour24}:{minute}{s}$",
            "time 12h": f"^{s}{hour12}:{minute}(:{second})?{s}$",
            "time 12h nosec": f"^{s}{hour12}:{minute}{s}$",
            "timestamp 24h": f"^{s}{hour24}:{minute}:{second}{s}$",
            "timestamp 12h": f"^{s}{hour12}:{minute}:{second}{s}$",
            "uuid": r"^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$",
            "ip address": r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$",
            "ipv4 address": r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$",
            # scary - but covers all cases
            "ipv6 address": r"^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$",
            "email": r"^[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%]+@[a-zA-ZÀ-ÿĀ-ſƀ-ȳ0-9.\-_%]+\.[A-Za-z]{2,4}$",
            "phone number": r"^((\+[0-9]{1,2}\s)?\(?[0-9]{3}\)?[\s.-])?[0-9]{3}[\s.-][0-9]{4}$",
            "credit card number": r"^[0-9]{14}|[0-9]{15}|[0-9]{16}|[0-9]{17}|[0-9]{18}|[0-9]{19}|([0-9]{4}-){3}[0-9]{4}|([0-9]{4} ){3}[0-9]{4}$",
        }

    @classmethod
    def is_numeric(cls, format):
        return format in [
            "integer",
            "positive integer",
            "negative integer",
            "decimal",
            "positive decimal",
            "negative decimal",
            "decimal point",
            "positive decimal point",
            "negative decimal point",
            "decimal comma",
            "positive decimal comma",
            "negative decimal comma",
            "percentage",
            "positive percentage",
            "negative percentage",
            "percentage point",
            "positive percentage point",
            "negative percentage point",
            "percentage comma",
            "positive percentage comma",
            "negative percentage comma",
            "money",
            "money point",
            "money comma",
        ]


class FormatCfg:

    default_formats: dict[str, str] = FormatHelper.build_default_formats()
