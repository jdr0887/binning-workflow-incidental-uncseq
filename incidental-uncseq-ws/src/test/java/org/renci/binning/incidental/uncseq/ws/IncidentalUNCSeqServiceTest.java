package org.renci.binning.incidental.uncseq.ws;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.junit.Test;
import org.renci.binning.core.incidental.IncidentalBinningJobInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

public class IncidentalUNCSeqServiceTest {

    @Test
    public void testSubmit() {

        List<Object> providers = new ArrayList<>();
        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true);
        mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        provider.setMapper(mapper);
        providers.add(provider);

        String restServiceURL = String.format("http://%1$s:%2$d/cxf/%3$s/%3$sService", "localhost", 8181, "IncidentalUNCSeq");

        WebClient client = WebClient.create(restServiceURL, providers).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON);

        IncidentalBinningJobInfo info = new IncidentalBinningJobInfo("jdr-test", "M", 10, 13);
        Response response = client.path("submit").post(info);
        String id = response.readEntity(String.class);
        System.out.println(id);

    }

    @Test
    public void scratch() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true);
            mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
            IncidentalBinningJobInfo info = new IncidentalBinningJobInfo("jdr-test", "M", 10, 13);
            String jsonInString = mapper.writeValueAsString(info);
            System.out.println(jsonInString);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

}
