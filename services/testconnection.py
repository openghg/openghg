def testconnection(args):
    from openghg.util import timestamp_now

    now_str = f"Function run at {str(timestamp_now())}"

    return {"results": now_str}
