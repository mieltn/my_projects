import imaplib
import email
import os

class EmailDownloader():

    connection = None
    username = None
    password = None

    def __init__(self):

        self.username = input('Enter email user: ')
        self.password = input('Enter password: ')
        self.connection = imaplib.IMAP4_SSL('imap.gmail.com', 993)
        self.connection.login(self.username, self.password)


    def fetch_msg(self, gmail_folder, sender, start_date, end_date):

        self.connection.select(gmail_folder)
        _, email_ids = self.connection.search(None,
                                              f'(SINCE {start_date})',
                                              f'(BEFORE {end_date})',
                                              f'(FROM {sender})')

        emails = []

        for num in email_ids[0].split():

            _, resp = self.connection.fetch(num, '(RFC822)')
            msg = email.message_from_bytes(resp[0][1])
            emails.append(msg)

        return emails


    def save_attach(self, msg, download_folder):

        for part in msg.walk():

            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()

            att_path = os.path.join(download_folder, filename)
            print(att_path)

            if not os.path.isfile(att_path):
                fp = open(att_path, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()

        return att_path

    
if __name__ == '__main__':
    
    attach_downloader = EmailDownloader()

    email_folder = input('Enter email folder: ')
    sender = input('Enter sender: ')
    start_date = input('Enter start date (day-month abbreviated name-yaer): ')
    end_date = input('Enter end date (day-month abbreviated name-yaer): ')
    download_folder = input('Enter download_folder: ')

    emails = attach_downloader.fetch_msg(email_folder, sender, start_date, end_date)

    for doc in emails:
    attach_downloader.save_attach(doc, download_folder)

