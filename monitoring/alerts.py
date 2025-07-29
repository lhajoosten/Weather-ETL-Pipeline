import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
import os

class AlertSystem:
    def __init__(self):
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.email_user = os.getenv("EMAIL_USER")
        self.email_password = os.getenv("EMAIL_PASSWORD")
        self.alert_recipients = os.getenv("ALERT_RECIPIENTS", "").split(",")
    
    def send_weather_alert(self, city: str, temperature: float, condition: str):
        """Send weather alert for extreme conditions"""
        if temperature > 35 or temperature < -10:
            subject = f"🌡️ Extreme Weather Alert - {city}"
            message = f"""
            Extreme weather detected in {city}:
            
            Temperature: {temperature}°C
            Condition: {condition}
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            Please take appropriate precautions.
            """
            self._send_email(subject, message)
    
    def send_pipeline_failure_alert(self, error_message: str):
        """Send alert when ETL pipeline fails"""
        subject = "🚨 ETL Pipeline Failure Alert"
        message = f"""
        The Weather ETL Pipeline has encountered an error:
        
        Error: {error_message}
        Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Please check the logs and take corrective action.
        """
        self._send_email(subject, message)
    
    def send_data_quality_alert(self, issues: list):
        """Send alert for data quality issues"""
        if issues:
            subject = "⚠️ Data Quality Issues Detected"
            message = f"""
            Data quality issues detected in the Weather ETL Pipeline:
            
            Issues:
            {chr(10).join(f"- {issue}" for issue in issues)}
            
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            
            Please review the data quality logs.
            """
            self._send_email(subject, message)
    
    def _send_email(self, subject: str, message: str):
        """Send email notification"""
        if not self.email_user or not self.alert_recipients:
            logging.warning("Email configuration incomplete. Alert not sent.")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_user
            msg['To'] = ", ".join(self.alert_recipients)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_user, self.email_password)
            text = msg.as_string()
            server.sendmail(self.email_user, self.alert_recipients, text)
            server.quit()
            
            logging.info("Alert email sent successfully")
        except Exception as e:
            logging.error("Failed to send email alert: %s", e)
