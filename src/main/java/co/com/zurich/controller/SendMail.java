package co.com.zurich.controller;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.imageio.ImageIO;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import co.com.zurich.config.ArchivoPropiedades;


public class SendMail {
	@SuppressWarnings("unused")
	public static String crearCorreo(String linea, String nomArchivo) throws Throwable {
		java.util.Date date = new Date();
			
		if (linea == null) {
			String Pagina =("<!DOCTYPE html PUBLIC \"-W3CDTD XHTML 1.0 TransitionalEN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\r\n" + 
					"<html xmlns=\"http://www.w3.org/1999/xhtml\">\r\n" + 
					"<head>\r\n" + 
					"<meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" />\r\n" + 
					"<title>Zurich</title>\r\n" + 
					"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>\r\n" + 
					"</head>\r\n" + 
					"<body style=\"margin: 0; padding: 0;\">\r\n" + 
					"\r\n" + 
					"\r\n" +
					"<b>"+
					"<font-family:\"Trebuchet MS, Verdana, Arial, Helvetica, sans-serif\"><br>\r\n" + 
					"Se ha procesado el archivo " + nomArchivo + " con resultado exitoso \r\n" + 
					"</font>\r\n" +
					"</b>"+
					"\r\n" + 
					"\r\n" + 
					"\r\n" + 
					"\r\n" + 
					"\r\n" + 
					"</body>\r\n" + 
					"</html>");
			
			return Pagina;
		}else {
			
			String Pagina =("<!DOCTYPE html PUBLIC \"-W3CDTD XHTML 1.0 TransitionalEN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\r\n" + 
					"<html xmlns=\"http://www.w3.org/1999/xhtml\">\r\n" + 
					"<head>\r\n" + 
					"<meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\" />\r\n" + 
					"<title>Zurich</title>\r\n" + 
					"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"/>\r\n" + 
					"</head>\r\n" + 
					"<body style=\"margin: 0; padding: 0;\">\r\n" + 
					"\r\n" + 
					"\r\n" + 
					
					"<font-family:\"Trebuchet MS, Verdana, Arial, Helvetica, sans-serif\"><br><br>\r\n" + 
					 "<br>" +
					"<b>"+
					"se ha procesado el archivo " + nomArchivo + " con resultado erroneo \r\n" +
					"</b>"+
					"<br>" +
					 linea + "\r\n" +
					"</font\r\n" + 
					"<br>\r\n" + 
					"\r\n" + 
					"\r\n" + 
					"\r\n" + 
					"</body>\r\n" + 
					"</html>");
		
			return Pagina;
		}
	}
	
	public static void enviarcorreo(String linea, String NomArchivo,String destinatario) throws Throwable {
		
		java.util.Date date = new Date();
		//String fecha = Asistente.fechas(date);
        //Optener la ip local
       // InetAddress address = InetAddress.getLocalHost();

        //Url de acceso
        //String urlimage = (System.getProperty("user.home")+"\\Documents\\Actualizacion TRM\\");
		String correo = ArchivoPropiedades.getPropiedades().getProperty("correoInicial");
		String contrasenia = ArchivoPropiedades.getPropiedades().getProperty("claveCorreo");
		String correoPrincipal = ArchivoPropiedades.getPropiedades().getProperty("correoAdmin");
		String smtp = ArchivoPropiedades.getPropiedades().getProperty("smtp");
        try {
            
            //Donde se enviarán los mensajes
    
            //String para1 = "jeferson.pedraza@samtel.co";
           // cuerpoMensaje = cuerpoMensaje + url;

            Properties props = System.getProperties();

            props.put("mail.smtp.host",smtp);
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.user", correo);
            props.put("mail.smtp.password", contrasenia);
            props.put("mail.smtp.port", "587");
        	props.put("mail.smtp.starttls.enable", "true");
        	props.put("mail.smtp.auth", "true");

            Session sesion = Session.getDefaultInstance(props);
            
            BodyPart text = new MimeBodyPart( ) ; 
            text.setContent(crearCorreo(linea, NomArchivo), "text/html" );
            BodyPart attached = new MimeBodyPart( ) ; 
          //  attached.setDataHandler(new DataHandler(new FileDataSource(urlimage+"TRM  "+fecha+"/Indicadores_"+fecha+" .jpg" ) ) ) ; 
          //  attached.setFileName("Indicadores_"+fecha+" .jpg" ) ; 
            MimeMultipart multiPart = new MimeMultipart( ) ; 
            multiPart.addBodyPart(text ) ;
            //multiPart.addBodyPart(attached ) ; 
            MimeMessage message = new MimeMessage(sesion);
            
            
         message.setFrom(new InternetAddress(correo));
         message.addRecipient(Message.RecipientType.TO, new InternetAddress(destinatario));
         message.addRecipient(Message.RecipientType.CC, new InternetAddress(correoPrincipal));
            if (linea != null) {
            	message.setSubject("Archivo "+ NomArchivo + " genero un error " );
			}else {
				message.setSubject("Archivo "+ NomArchivo + " procesado con exito " );
			}
            
            message.setContent(multiPart);
            

            Transport transport = sesion.getTransport("smtp");
           
            transport.connect(smtp, correo, contrasenia);
            transport.sendMessage(message, message.getAllRecipients());
            System.out.println("correo enviado");
            transport.close();
            

        } catch (MessagingException e) {
            System.out.println("Error enviarCorreo: " + e.getMessage());
            
        }
}




	
	
}
