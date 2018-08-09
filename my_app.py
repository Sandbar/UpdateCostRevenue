

from flask import Flask, request
from update_basic_dataset import UpdateCostRevenue


app = Flask(__name__)


@app.route('/index.html', methods=['GET', 'POST'])
def main():
    if request.method == 'GET':
        request_sign = request.values.get('update')
        ucr = UpdateCostRevenue()
        ucr.logger.info('receiving parameter ' + request_sign)
        if request_sign == 'ok':
            ucr.logger.info('start update')
            res_str = ucr.main()
            return res_str
        else:
            return 'update is defeated!'
    else:
        return 'POST!!!'


if __name__ == '__main__':
    app.run('0.0.0.0', port=5555)

