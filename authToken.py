"""
1. get creds
2. get auth_code response url
3. login and get auth code from url
4. generate access token from auth_code
"""
from fyers_apiv3 import fyersModel
import seleniumbase as sb
import time
from selenium.webdriver.common.by import By
import pyotp
import nodriver as uc
import os
import json
import dotenv



# question to ponder: is making a separate class for this really necessary?
class Login():
    """
    for manual intervention, you can use the _generate_response_url and get the authcode manually,set the variable to it and run get_access_token
    """

    def __init__(self,client_id,secret_key, redirect_uri,key=None,phoneno=None,TOTPseckey=None,*args,**kwargs):
        super().__init__()
        self.client_id = client_id
        self.secret_key = secret_key
        self.redirect_uri = redirect_uri
        self.four_digit_key = key
        self.phoneno = phoneno
        self.TOTPseckey = TOTPseckey
        self.response_type = "code"  
        self.state = "sample_state"
        self.grant_type = "authorization_code"
        self.auth_code = None
        self.responseurl = None
        self.access_token = ''

    def _generate_response_url(self):
        """ gets auth_code"""
        self.session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type=self.response_type,
            grant_type=self.grant_type
        )
       
        # Generate the auth code using the session model
        self.responseurl = self.session.generate_authcode()
        return self.responseurl
    

    def _login_and_get_auth(self,response,driver_mode=0):
        """automatically gets auth_code from response url"""


        if self.TOTPseckey == None:
            return KeyError('TOTPseckey not provided')
        if self.four_digit_key == None:
            return KeyError('four digit key not provided')
        if self.phoneno == None:
            return KeyError('phoneno not provided')
        

        try:
            """
            if driver_mode == 0:
                drive = sb.SB(uc=True)
            elif driver_mode == 1:
                drive = sb.Driver(undetectable=True)
            drive.get(response)"""
            if driver_mode == 0:
                drive = sb.Driver(uc=True,incognito=True)  # Initialize BaseCase
                #time.sleep(2)
                drive.uc_open_with_reconnect(response,reconnect_time=5)
            elif driver_mode == 1:
                drive = sb.Driver(undetectable=True,incognito=True)  # Initialize BaseCase
                time.sleep(2)
                drive.uc_open_with_reconnect(response,reconnect_time=5)

            phno = drive.find_element(By.XPATH,'/html/body/section[1]/div[3]/div[3]/form/div[1]/div/input')
            drive.uc_click('/html/body/section[1]/div[3]/div[3]/form/div[1]/div/input',By.XPATH)
            #sending phone number details
            phno.send_keys(self.phoneno)

            #click continue 
            drive.uc_click('/html/body/section[1]/div[3]/div[3]/form/button',By.XPATH)
            time.sleep(2) #to allow the webpage to load

            #TOTP section
            otp = pyotp.TOTP(self.TOTPseckey).now()
            for i in range(1,7):
                drive.find_element(By.XPATH,f'/html/body/section[6]/div[3]/div[3]/form/div[3]/input[{i}]').send_keys(otp[i-1])

            #pressing continue
            drive.uc_click('/html/body/section[6]/div[3]/div[3]/form/button',By.XPATH)

            time.sleep(1)
            # PIN section
            for i in range(1,5):
                drive.find_element(By.XPATH,f'/html/body/section[8]/div[3]/div[3]/form/div[2]/input[{i}]').send_keys(self.four_digit_key[i-1])
            drive.uc_click('/html/body/section[8]/div[3]/div[3]/form/button',By.XPATH)

            time.sleep(1)
            try:
                # terms and conditions validation 
                drive.uc_click('/html/body/div/div/div/div/div/div[3]/div/div[3]/label',By.XPATH)
                drive.uc_click('/html/body/div/div/div/div/div/div[4]/div/a[2]/span',By.XPATH)
            except Exception as e:
                print('no neeed to validate :)')
            #time.sleep(2)
            url = drive.current_url
            return url.split('&')[2].split('=')[1]
        finally:
            drive.quit()
        




    def get_access_token(self):
        # generating response url if not done manually
        if not self.responseurl:
            response = self._generate_response_url()
        
        #getting auth_code if we did not do it manually
        if not self.auth_code: 
            try:
                self.auth_code= self._login_and_get_auth(response)               # trying with uc mode in seleniumbase
                print('auth_code obtained!')
            except Exception as e :
                print(f'shucks login issue occured, we prolly got detected: {e}')
                print('mode 0 used up')
                try:
                    self.auth_code= self._login_and_get_auth(response,driver_mode=1)     # trying with undected mode in seleniumbase
                    print('auth_code obtained!')
                except Exception as e:
                    print(f'auth code failed to be received {e}')
                    print('mode 1 undetectable=true gone')
                    return NotImplementedError('bro SeleniumBase might be failing you look for alternatives')

        # getting access token
        """outputs access token """
        # Set the authorization code in the session object
        self.session.set_token(self.auth_code)
        # Generate the access token using the authorization code
        response = self.session.generate_token()
        print(response['code'])
        if response['s']=='ok':
            self.access_token = response['access_token']
            
            print('*'*80)
            print('success')
            return self.access_token
        else:
            return NotImplementedError('bro you fucked up somewhere'+str(response['code']))


# autologin by saving data in secrets.txt file
class AutoLogin(Login):
    def __init__(self,client='0'):
        dotenv.load_dotenv()
        creds = json.loads(os.getenv(client))
        super().__init__(client_id=creds['client_id'],
                         secret_key=creds['secret_key'],
                         redirect_uri=creds['redirect_uri'],
                         key=creds['key'],
                         phoneno=creds['phoneno'],
                         TOTPseckey=creds['TOTPseckey'])

