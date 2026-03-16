import pywinauto


def focus(title: str):
    app = pywinauto.Application()
    a = pywinauto.findwindows.find_elements()
    for aa in a:
        if title in aa.name:
            app.connect(handle=aa.handle)
            app.window(handle=aa.handle).set_focus()


if __name__ == "__main__":
    focus("삼성증권")
