from telegram import Bot, ParseMode


class simpleTelegram:
    def __init__(self, token: str, userid: str):
        self.telegram_req = "https://api.telegram.org/bot" + token + "/getUpdates"
        self.bot = Bot(token=token)
        self.userid = userid

    def sendMsg(self, msg: str, reply_markup=None) -> str:
        if not reply_markup == None:
            req = self.bot.send_message(
                chat_id=self.userid, text=str(msg), reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
        else:
            req = self.bot.send_message(
                chat_id=self.userid, text=str(msg), parse_mode=ParseMode.HTML)
        return req["message_id"]

    def sendGIF(self, msg: str, imgPth: str):
        req = self.bot.send_animation(chat_id=self.userid, animation=open(imgPth, 'rb'), caption=msg)
        return req["message_id"]

    def delMsg(self, message_id: str):
        try:
            self.bot.delete_message(chat_id=self.userid, message_id=message_id)
        except Exception as e:
            print(f"Message {message_id} 삭제 실패 => {e}")

    def sendPhoto(self, imgPth: str) -> str:
        try:
            req = self.bot.send_photo(
                chat_id=self.userid, photo=open(imgPth, "rb"))
        except Exception as e:
            req = {"message_id": self.sendMsg(msg=f"Error while sending photo: {e}")}
        return req["message_id"]

    def editMsg(self, messga_id: str, msg: str):
        try:
            self.bot.edit_message_text(
                chat_id=self.userid, message_id=messga_id, text=msg
            )
        except Exception as e:
            print(f"Message {messga_id} 수정 실패 => {e}")