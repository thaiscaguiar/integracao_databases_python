class Venda:

    def __init__(self, nota_fiscal, vendedor, total):
        self._nota_fiscal = nota_fiscal
        self._vendedor = vendedor
        self._total = total

    @property
    def id(self):
        return self._id 

    @id.setter
    def id(self, new_id):
        self._id = new_id

    @id.deleter
    def id(self):
        del self._id

    @property
    def nota_fiscal(self):
        return self._nota_fiscal

    @nota_fiscal.setter
    def nota_fiscal(self, new_nota_fiscal):
        self._nota_fiscal = new_nota_fiscal

    @nota_fiscal.deleter
    def nota_fiscal(self):
        del self._id

    @property
    def vendedor(self):
        return self._vendedor

    @vendedor.setter
    def vendedor(self, new_vendedor):
        self._vendedor = new_vendedor

    @vendedor.deleter
    def vendedor(self):
        del self._vendedor

    @property
    def total(self):
        return self._total

    @total.setter
    def total(self, new_total):
        self._total = new_total

    @total.deleter
    def total(self):
        del self._total

