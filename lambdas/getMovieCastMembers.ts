import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
  GetCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    const queryParams = event.queryStringParameters;
    if (!queryParams) {
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing query parameters" }),
      };
    }
    if (!queryParams.movieId) {
      return {
        statusCode: 500,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ message: "Missing movie Id parameter" }),
      };
    }

    const movieId = parseInt(queryParams.movieId);
    const includeFacts = queryParams.facts === "true";

    // Query for cast members
    let castCommandInput: QueryCommandInput = {
      TableName: process.env.CAST_TABLE_NAME,
      KeyConditionExpression: "movieId = :m",
      ExpressionAttributeValues: {
        ":m": movieId,
      },
    };

    // Add filter expressions for other optional fields if provided
    let filterExpressions: string[] = [];
    if ("roleName" in queryParams) {
      filterExpressions.push("begins_with(roleName, :r)");
      castCommandInput.ExpressionAttributeValues = castCommandInput.ExpressionAttributeValues || {};
      castCommandInput.ExpressionAttributeValues[":r"] = queryParams.roleName;
    }
    else if ("actorName" in queryParams) {
      filterExpressions.push("begins_with(actorName, :a)");
      castCommandInput.ExpressionAttributeValues = castCommandInput.ExpressionAttributeValues || {};
      castCommandInput.ExpressionAttributeValues[":a"] = queryParams.actorName;
    }

    if (filterExpressions.length > 0) {
      castCommandInput.FilterExpression = filterExpressions.join(" AND ");
    }

    const castCommandOutput = await ddbDocClient.send(
      new QueryCommand(castCommandInput)
    );

    let response: { cast: Record<string, any>[] | undefined; movie?: { title: string; genre_ids: number[]; overview: string } } = {
      cast: castCommandOutput.Items,
    };

    // If facts=true, query for movie metadata
    if (includeFacts) {
      const movieCommandInput: GetCommandInput = {
        TableName: process.env.TABLE_NAME,
        Key: { id: movieId },
      };

      const movieCommandOutput = await ddbDocClient.send(
        new GetCommand(movieCommandInput)
      );

      if (movieCommandOutput.Item) {
        response = {
          ...response,
          movie: {
            title: movieCommandOutput.Item.title,
            genre_ids: movieCommandOutput.Item.genre_ids,
            overview: movieCommandOutput.Item.overview,
          },
        };
      }
    }

    return {
      statusCode: 200,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(response),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
