package learn.jerseyProject.jersey;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

@Path("/")
public class RestFulHandle {

    @GET
    @Path("namespace/{namespace}")
    // @Produces(MediaType.APPLICATION_JSON)
    public Response getUser(@PathParam("namespace") String namespace) {
        return Response.status(200).entity("It is" + namespace).build();
    }

}
