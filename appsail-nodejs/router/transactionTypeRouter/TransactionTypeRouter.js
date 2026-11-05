import Express from "express";
import {
  getTransactionTypeList,
  addTransactionType,
} from "../../controller/transactionType/transactionTypeController.js";

const router = Express.Router();

router.get("/list", getTransactionTypeList);
router.post("/add", addTransactionType);

export default router;
