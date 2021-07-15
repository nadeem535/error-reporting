# -*- coding: utf-8 -*-
from email.message import EmailMessage
from email.utils import make_msgid
#from email.mime.text import MIMEText
#from email.mime.multipart import MIMEMultipart, MIMEBase
#from email import encoders
import mimetypes
import smtplib
import seaborn as sns

def sendmail(errdata, err_dt):
    ## Email the data now
    msg = EmailMessage()
    #msg = MIMEMultipart()
    
    def color_alert_red(value):
      """
      Colors elements in a dateframe
      green if positive and red if
      negative. Does not color NaN
      values.
      """
    
      if value > 5000:
        color = 'red'
      elif value < 500:
        color = 'green'
      else:
        color = 'black'
    
      return 'color: %s' % color
    

    # Set colormap equal to seaborns light green color palette
    cm = sns.light_palette("yellow", as_cmap=True)
    #cm = sns.choose_dark_palette(input="rgb",as_cmap=True)
    #cm = sns.choose_light_palette(input="rgb",as_cmap=True)
    
#    df = errdata.to_html()
    html = (
        errdata.style
    #    .format(percent)
        .hide_index()
        .applymap(color_alert_red, subset=['Counts'])
    #    .set_properties(**{'font-size': '12pt', 'font-family': 'Arial'})
        .set_properties(**{'border-color': 'black','border-style':'solid','border-width': '0px'})
    #    .bar(subset=['col4', 'col5'], color='lightblue')
        .background_gradient(cmap=cm, subset=['Counts'])
    #    .set_table_styles(df_styles)
        .render()
    )
    
    #msg = MIMEMultipart("alternative",None, [MIMEText(df,'html')])
    strFrom = 'CH_ERR_Reporting@optum.com'
    strTo = ['CHSSS_DL@ds.uhc.com']
#    strTo = ['nadeemuddin@optum.com','gupta.ankit@optum.com']
    strCc = ['nadeemuddin@optum.com','gupta.ankit@optum.com']
    
    #generic email headers
    msg['Subject'] = err_dt + ' | CH Post/Accum/EOB Errors alert'
    msg['From'] = strFrom
    msg['To'] = strTo
    msg['Cc'] = strCc
    
    # set the plain text body
    msg.set_content("set content ----Below is today's Post/Accum/EOB error grouped by CLM_SYS_ID and ERR_CD" )
    
    #msg.attach(MIMEText(df,'html'))
    
    # now create a Content-ID for the image image_cid = make_msgid(domain='optum.com') # if `domain` argument isn't provided, it will 
    # use your computer's name
    image_cid = make_msgid()
    image_cid_sys = make_msgid()
    # set an alternative html body
    
    msg.add_alternative("""<html>
        <body>
            <p>Hi Team,<br>
            Below are yesterday's TOP 10 "Post-adj/Accum/EOB" error grouped by CLM_SYS_ID and ERR_CD.<br>
            </p>{0}
             <img src="cid:{image_cid_sys}">
             <img src="cid:{image_cid}">
            <p>
            </p>
        </body>
    </html>
    """.format(html, image_cid_sys=image_cid_sys[1:-1], image_cid=image_cid[1:-1]),subtype='html')
    # image_cid looks like <long.random.number@xyz.com> to use it as the img src, we don't need `<` or `>` so we use [1:-1] to strip them off
    
    # now open the image and attach it to the email
    imgpath = "./plots/"+err_dt+"-plot.png"
    imgpathcae = "./plots/"+err_dt+"-caeplot.png"
    with open(imgpath, 'rb') as img:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
        # attach it
        msg.get_payload()[1].add_related(img.read(), 
                                             maintype=maintype, 
                                             subtype=subtype, 
                                             cid=image_cid)
    with open(imgpathcae, 'rb') as img_sys:
        maintype, subtype = mimetypes.guess_type(img_sys.name)[0].split('/')
        msg.get_payload()[1].add_related(img_sys.read(), 
                                             maintype=maintype, 
                                             subtype=subtype, 
                                             cid=image_cid_sys)
    
    #msg.attach(MIMEText(df, 'html'))
    # the message is ready now you can write it to a file or send it using smtplib
    # Send the email (this example assumes SMTP authentication is required)
    smtp = smtplib.SMTP()
    smtp.connect('mailo2.uhc.com')
    #smtp.login('exampleuser', 'examplepass')
    smtp.sendmail(strFrom, strTo, msg.as_string())
    #smtp.send_message(msg)
    smtp.quit()
