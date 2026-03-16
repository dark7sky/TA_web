def to_qmenu(xkey: str) -> str:
    if xkey == "!":
        return "shift-1"
    elif xkey == "@":
        return "shift-2"
    elif xkey == "#":
        return "shift-3"
    elif xkey == "$":
        return "shift-4"
    elif xkey == "%":
        return "shift-5"
    elif xkey == "^":
        return "shift-6"
    elif xkey == "&":
        return "shift-7"
    elif xkey == "*":
        return "shift-8"
    elif xkey == "(":
        return "shift-9"
    elif xkey == ")":
        return "shift-0"
    elif xkey == "_":
        return "shift--"
    elif xkey == "+":
        return "shift-="
    else:
        return xkey
