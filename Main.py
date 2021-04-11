import time

import requests
from bs4 import BeautifulSoup
import json
import pandas
import logging
import math
import traceback


# Constants
walmartGroceryCode = "10019"
walmartProductCodeURL = "https://www.walmart.ca/search?c=" + walmartGroceryCode + "&q="
walmartPriceURL = "https://www.walmart.ca/api/product-page/v2/price-offer"
apiKey = "b8ccb6bfb4f3438023a5fb044d4ce130"
scrapperBaseURL = "http://api.scraperapi.com?api_key=b8ccb6bfb4f3438023a5fb044d4ce130&url="

# Logger config
logging.basicConfig(filename='app.log', filemode='w',
                    format='%(levelname)s - %(message)s', level=logging.ERROR)

headers = {
    'Host': 'www.walmart.ca',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'Accept': "*/*",
    'Accept-Encoding': "gzip, deflate, br",
    'Accept-Language': "en-CA,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,fr;q=0.6",
    'Referer': "https://www.scraperapi.com/",
    'Sec-Ch-Ua-Mobile': "?0",
    'Sec-Fetch-Dest': "document",
    'Sec-Fetch-Mode': "navigate",
    'Sec-Fetch-Site': "cross-site",
    'Sec-Fetch-User': "?1",
    'Upgrade-Insecure-Requests': "1",
    'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    'X-Amzn-Trace-Id': "Root=1-5ffbb102-231af6f93324a1567a7a2957",
}


def callScrapperWithProductCode(productCode):
    print('Beginning GET request...')
    walmartProductCodeURL = "https://www.walmart.ca/search?c=10019&q=" + productCode
    scrapperURL = scrapperBaseURL + walmartProductCodeURL

    response = requests.request("GET", scrapperURL, headers=headers)

    if response.status_code == 412:
        print('inside 412')
        httpErrorMsg = 'Error from HTTP GET request. Blo cked by walmart API.'
        raise HttpException(httpErrorMsg)
    elif response.status_code == 500:
        httpErrorMsg = 'Error from HTTP GET request. 500 status code by walmart API.'
        raise HttpException(httpErrorMsg)
    else:
        soup = BeautifulSoup(response.text, 'html.parser')
        allScripts = soup.find_all('script')
        print("GET request complete. Status code:", response.status_code)
        return soup, allScripts


def getProductPrice(skuId, postPayload):
    print('Beginning POST request...')

    scrapperPostURL = scrapperBaseURL + walmartPriceURL

    postResponse = requests.request(
        "POST", scrapperPostURL, headers=headers, data=json.dumps(postPayload))

    print(postResponse)

    if postResponse.status_code == 412:
        httpErrorMsg = 'Error from HTTP POST request. Blocked by walmart API.'
        raise HttpException(httpErrorMsg)
    elif postResponse.status_code == 500:
        httpErrorMsg = 'Error from HTTP POST request. 500 status code by walmart API.'
        raise HttpException(httpErrorMsg)
    elif postResponse.status_code == 502:
        httpErrorMsg = 'Error from HTTP POST request. 502 status code by walmart API.'
        raise HttpException(httpErrorMsg)
    else:
        price = json.loads(postResponse.text)
        print("Post request complete. Status code:", postResponse.status_code)
        return price['offers'][skuId[0]]['currentPrice']


def extractFullfillmentId(soup, scripts):
    scriptOne = soup.find('script', {'id': 'tb-djs-wml-data'}).string
    scriptOneJson = json.loads(scriptOne)

    # # Get the key necesary for data
    scriptOneRelevantData = scriptOneJson['ccm']['common']['features']['defaultLPStore']
    scriptOneRelevantDataJson = json.loads(scriptOneRelevantData)
    fullfillmentStoreId = scriptOneRelevantDataJson['fulfillmentStoreId']
    return fullfillmentStoreId


# Find the script with relevant data
def findRelevantScript(scripts):

    try:
        for script in scripts:
            # Verify that this is best way
            if '__PRELOADED_STATE__' in script.string:
                scriptString = script.string
                splitString = scriptString.split('window.__PRELOADED_STATE__=')
                data = splitString[1]
                removeSemiColon = data.split(';')
                finalData = removeSemiColon[0]

                jsonData = json.loads(finalData)
                return jsonData
    except Exception as e:
        return None
        print(e, 'error in find relevant scripts')


def remainingArgs(scripts):

    scriptsWithKeyData = findRelevantScript(scripts)

    if not scriptsWithKeyData:
        raise Exception(
            "No script found when searching for window.__preloaded_state string inside scriptsWithKeyData function")

    # print(json.dumps(scriptsWithKeyData, indent=4, sort_keys=True))
    try:

        if not scriptsWithKeyData['results']['entities']['productIds']:
            raise ProductIdNotFound('Product ID list is empty')

        productId = scriptsWithKeyData['results']['entities']['productIds'][0]
        productInfo = scriptsWithKeyData['results']['entities']['products'][productId]
        skuId = productInfo['skuIds']
        experience = scriptsWithKeyData['common']['experience']
        searchQuery = scriptsWithKeyData['common']['searchQuery']
        return productId, skuId, experience, searchQuery
    except ProductIdNotFound as pe:
        raise pe
    except Exception as err:
        print(err, 'in remaining args****')
        raise Exception('Error occured in remaining args functions')


def createPostRequest(fullfillmentID, productId, skuId, experience, searchQuery):
    payloadForPostRequest = {
        "fsa": "L5V",
        "products": [
            {
                "productId": productId,
                "skuIds": skuId
            }
        ],
        "lang": "en",
        "pricingStoreId": fullfillmentID,
        "fulfillmentStoreId": fullfillmentID,
        "experience": experience
    }
    return payloadForPostRequest


def extractPostArgsInfoFromScripts(soup, allScripts):
    fullfillmentID = extractFullfillmentId(soup, allScripts)

    productId, skuId, experience, searchQuery = remainingArgs(allScripts)

    payloadForPostRequest = createPostRequest(
        fullfillmentID, productId, skuId, experience, searchQuery)

    return skuId, payloadForPostRequest


def validateProductCode(productCode):
    # Some values are nans, we only care about the strings.
    if isinstance(productCode, str):
        # Remove apostrophe and extra space and a "v" letter
        reformatProductCode = productCode.replace(
            "'", " ").replace(" ", "").replace("v", "")
        # Catch failures to cast to int.
        try:
            convertToInt = int(reformatProductCode)
            # Now since we only care about bar codes which are numbers, we can cast it as an int, and then check if it is an instance of an int type.
            if isinstance(convertToInt, int):
                # Finally we return string version of the bar code.
                return reformatProductCode
        except Exception as ex:
            # Return will be  NONE
            # Uncomment to see which failed to be parsed as int.
            pass

# CUSTOM ERRORS


class Error(Exception):
    pass


class ProductIdNotFound(Error):
    pass


class HttpException(Error):
    pass

# To test with 50 products


def RunProgram():
    # Load csv and extract name, product id and cost
    datafile = pandas.read_csv('products.csv')

    tempDataFile = pandas.read_csv('products.csv')[
        ['Title', 'Variant Barcode', 'Cost per item']]

    tempDataFile = tempDataFile.set_index('Title', drop=False)

    tempDataFile = tempDataFile.to_dict("records")

    # Index to keep track of row.
    index = 1337

    for x in tempDataFile:

        x = tempDataFile[index]
        productTitle = x['Title']
        productCode = x['Variant Barcode']
        apiRequestAttempts = 5
        currentFileRow = str(index + 2)

        print('----------------Beginning process-----------------')

        if isinstance(productTitle, str) == False:
            file = open(
                "ListOfProductCodesThatAreNullOrNotValidInCsv.txt", "a")
            file.write(currentFileRow + '\n')
            file.close()
            logging.error(
                "Skipping product... Invalid product in row:" + currentFileRow)
            index += 1
            continue

        print('Product Name:', productTitle)
        print('File row:', currentFileRow)  # Excel file begins at row 2.

        validatedProductCode = validateProductCode(productCode)
        print('Product Code', validatedProductCode)

        # Next iteration if invalid product code.
        if validatedProductCode is None:
            file = open(
                "ListOfProductCodesThatAreNullOrNotValidInCsv.txt", "a")
            file.write(currentFileRow + '\n')
            file.close()
            logging.error(
                "Skipping product... Invalid product:" + productTitle)
            index += 1
            continue

        # Try catch for the for loop api attempts.
        for attempts in range(apiRequestAttempts):
            try:
                # -----GET request
                apiResponse = callScrapperWithProductCode(validatedProductCode)

                soup = apiResponse[0]  # Contains HTML parsed.
                # Contains the data we need from the html
                htmlScriptData = apiResponse[1]

                # Gets the data for the POST request payload
                skuId, postArgs = extractPostArgsInfoFromScripts(
                    soup, htmlScriptData)

                # -----POST request
                productPrice = getProductPrice(skuId, postArgs)

                print('Old price', datafile.loc[index, 'Cost per item'])
                # logging.info("Old price:", datafile.loc[index, 'Cost per item'])
                datafile.loc[index, 'Cost per item'] = productPrice
                print('New price', datafile.loc[index, 'Cost per item'])
                print('Successfully updated the price for product code:',
                      validatedProductCode, 'row:', currentFileRow)
                logging.info(
                    'Successfully updated the price for product code:' + validatedProductCode)

                print('Old variant price',
                      datafile.loc[index, 'Variant Price'])
                # Add 20% to product price.
                productPriceTwentyPercent = productPrice * 0.20
                variantPrice = round(
                    (productPriceTwentyPercent + productPrice), 2)
                datafile.loc[index, 'Variant Price'] = variantPrice
                print('New variant price',
                      datafile.loc[index, 'Variant Price'])

                print('----------------ending process-----------------')

                # At this point we can safely assume that the GET, POST and the UPDATE in the excel file were done.
                break
            except ProductIdNotFound as pe:
                print(pe)
                file = open(
                    "ListOfInvalidProductCodesThatDontExistOnWalmart.txt", "a")
                file.write(str(productCode) + '\n')
                file.close()
                logging.error("Product code invalid:" + productCode +
                              '  Skipping...' + 'row number:' + currentFileRow)
                break
            except HttpException as he:
                print(he)
                if attempts < apiRequestAttempts - 1:
                    print(attempts, apiRequestAttempts)
                    print('Failed extracting the data... attempting again.')
                else:
                    print('Get request failed after 5 tries... Moving to next product')
                    logging.error('Get Request failed after 5 tries. - Product code:' +
                                  validatedProductCode + 'row number:' + currentFileRow)
                    continue
                # WAIT 20seconds before next attempt
                # time.sleep(20)
            except Exception as e:
                print(e, '*************GLOBAL ERROR ******************')
                logging.error('Unexpected error with product code:' +
                              productCode + 'row number:' + currentFileRow)
                continue

        index += 1

    datafile.to_csv("updated-products.csv", index=False)


# STARTS THE PROGRAM.
RunProgram()
