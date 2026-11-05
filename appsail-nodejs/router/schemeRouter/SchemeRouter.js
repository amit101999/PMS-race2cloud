import Express from "express";
import { getSchemeList, addScheme } from "../../controller/scheme/schemeController.js";

const router = Express.Router();

router.get("/list", getSchemeList);
router.post("/add", addScheme);

export default router;
