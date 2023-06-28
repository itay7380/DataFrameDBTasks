from flask_restx import Resource, fields, Api, Namespace, model
from flask import Blueprint, request
from http import HTTPStatus
from ....execution.CIndustrial import CIndustrial


_BPSum = Blueprint("BPSum", __name__)
_ApiSum = Api(_BPSum)
_NameSpaceSum = _ApiSum.namespace("sum")


_sumModel = _NameSpaceSum.model(
    name="sum",
    model={
        "category": fields.String,
        "industry": fields.String,
    }
)


@_NameSpaceSum.route("/")
class CSumValues(Resource):

    @_NameSpaceSum.expect(_sumModel)
    def post(self):
        try:
            dData = request.json
            sIndustry = dData["industry"]
            sCategory = dData["category"]
            Industrial = CIndustrial()
            nSumValues = int(Industrial.SumValues(sIndustry, sCategory))
            return {"sum_values": nSumValues}, HTTPStatus.OK.value
        except TypeError as ex:
            return str(ex), HTTPStatus.NOT_FOUND.value
        except BaseException as ex:
            return str(ex), HTTPStatus.INTERNAL_SERVER_ERROR.value
