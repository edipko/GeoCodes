package com.spotonresponse.webservices;


import org.json.JSONObject;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

// With @WebServlet annotation the webapp/WEB-INF/web.xml is no longer required.
@WebServlet(name = "fips5", value = "/fips5")
public class Fips5Engine extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        String code = request.getParameter("code");

        JSONObject jo = new JSONObject();
        jo.put("County", "Some County Name");
        jo.put("FIPS5 code", code);
        jo.put("Code Type", "FIPS5");
        jo.put("Polygon Type", "Multi-Polygon");
        jo.put("Multi-Polygon Segments", "5");
        jo.put("Polygon", "[[-136.123, 78.123, -136.143, 78.123, -136.199, 78.188, -136.123, 78.123], [-135.123, 78.123, -135.143, 78.123, -135.199, 78.188, -135.123, 78.123]]");

        response.setContentType("application/json");
        String output = jo.toString(3);

        response.getWriter().println(output);
    }
}