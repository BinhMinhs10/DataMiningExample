package requestapi;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;

//source: https://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
public class HttpURLConnectionExample implements Serializable {
    private final String USER_AGENT = "SFive/73.0";
    private static HttpURLConnection con;

    public static void main(String[] args)  {
        HttpURLConnectionExample http = new HttpURLConnectionExample();

        System.out.println("Testing 2 - Send Http POST request");
        http.sendPost();


        System.out.println("\nTesting 1 - Send Http GET request");
        http.sendGet();
    }

    private void sendPost() {

        // be carefull proxy and user agent SFive
        String url = "http://localhost:8080/products";
        String urlParameters = "{\n" +
                "\"id\": 4,\n" +
                "\"name\": \"Thanh xuan like a cup of tea\"\n" +
                "}";

        try{
            URL myurl = new URL(url);
            con = (HttpURLConnection) myurl.openConnection();

            // add request header
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", USER_AGENT);
            con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
            // header send
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            try{
                // send post request
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(urlParameters);
                wr.flush();
                wr.close();
            }catch (Exception e){
                e.getMessage();
            }


            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'POST request to URL: " + url);
            System.out.println("Post parameters: " + urlParameters);
            System.out.println("Response Code: " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            System.out.println("Response Message: "+response.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            con.disconnect();
        }

    }


    // HTTP GET request
    private void sendGet() {

        String url = "http://localhost:8080/products";



        try{
            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", USER_AGENT);

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'GET' request to URL : " + url);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            //print result
            System.out.println(response.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            con.disconnect();
        }


    }

}
